open Async.Std

module Make (Job : MapReduce.Job) = struct

  (* see .mli *)
  let run r w =
    match (WorkerRequest(Job).receive r) with
    | 'Ok MapRequest input -> return WorkerResponse(Job).send w (Job.map input)
    | 'Ok ReduceRequest input -> return WorkerResponse(Job).send w (Job.reduce input)
    (* finish processing all inputs/jobs *)
    | 'Eof -> return ()
end

(* see .mli *)
let init port =
  Tcp.Server.create
    ~on_handler_error:`Raise
    (Tcp.on_port port)
    (fun _ r w ->
      Reader.read_line r >>= function
        | `Eof    -> return ()
        | `Ok job -> match MapReduce.get_job job with
          | None -> return ()
          | Some j ->
            let module Job = (val j) in
            let module Worker = Make(Job) in
            Worker.run r w
    )
    >>= fun _ ->
  print_endline "server started";
  print_endline "worker started.";
  print_endline "registered jobs:";
  List.iter print_endline (MapReduce.list_jobs ());
  never ()


