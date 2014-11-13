open Async.Std
open Protocol

module Make (Job : MapReduce.Job) = struct
  module Request = WorkerRequest(Job)
  module Response = WorkerResponse(Job)

  (* see .mli *)
  let rec run r w =
    Request.receive r >>= function
    | `Ok Request.MapRequest input ->
      (try_with (fun () -> Job.map input) >>| function
      | Core.Std.Ok res -> Response.send w (Response.MapResult res); run r w
      | Core.Std.Error _ -> Response.send w (Response.JobFailed "Map failed"))
    | `Ok Request.ReduceRequest (key, inter_lst) ->
      (try_with (fun () -> Job.reduce (key, inter_lst)) >>| function
      | Core.Std.Ok res -> Response.send w (Response.ReduceResult res); run r w
      | Core.Std.Error _ -> Response.send w (Response.JobFailed "Reduce failed"))
    (* finish processing all inputs/jobs *)
    | `Eof -> failwith "worker closing connection."
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


