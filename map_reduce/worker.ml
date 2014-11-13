open Async.Std
open Protocol

module Make (Job : MapReduce.Job) = struct
  module Request = WorkerRequest(Job)
  module Response = WorkerResponse(Job)

  (* see .mli *)
  let rec run r w =
    print_string "working run"; 
    Request.receive r >>= function
    | `Ok Request.MapRequest input ->
      Job.map input >>=
      (fun res -> Response.send w (Response.MapResult res); run r w)
    | `Ok Request.ReduceRequest (key, inter_lst) ->
      Job.reduce (key, inter_lst) >>=
      (fun res -> Response.send w (Response.ReduceResult res); run r w)
    (* finish processing all inputs/jobs *)
    | `Eof -> return ()
end

(* see .mli *)
let init port =
  Tcp.Server.create
    ~on_handler_error:`Raise
    (Tcp.on_port port)
    (fun _ r w ->
      Reader.read_line r >>= function
        | `Eof    ->  return ()
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


