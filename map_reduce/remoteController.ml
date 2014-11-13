open Async.Std
open AQueue
open Protocol

let addr = ref []

let num_workers = ref 0

let init addrs = addr := addrs; num_workers := List.length !addr; ()



exception InfrastructureFailure
exception MapFailure of string
exception ReduceFailure of string

module Make (Job : MapReduce.Job) = struct
  (* Remember that map_reduce should be parallelized. Note that [Deferred.List]
     functions are run sequentially by default. To achieve parallelism, you may
     find the data structures and functions you implemented in the warmup
     useful. Or, you can pass [~how:`Parallel] as an argument to the
     [Deferred.List] functions.*)
	module Request = WorkerRequest(Job)
	module Response = WorkerResponse(Job)
	module C = Combiner.Make(Job)

	let map_reduce inputs =
  	(* create queue of workers *)
  	let queue = AQueue.create () in
	  Deferred.List.map ~how:`Parallel !addr
		  ~f:(fun (host, port) ->
		  	Tcp.connect (Tcp.to_host_and_port host port) >>= 
		  	(fun (sock, r, w) ->
		  		Writer.write_line w Job.name; 
		  		return(AQueue.push queue (r, w)))
			) >>= fun _ ->

		(* map phase: send input to workers *)
		Deferred.List.map ~how:`Parallel inputs
			~f:(fun input -> AQueue.pop queue >>=
				(fun (r, w) ->
					print_string "mapping";
					Request.send w (Request.MapRequest input);
					Response.receive r >>= function
						| `Ok (Response.MapResult res) -> print_string "a";
							AQueue.push queue (r, w); return res
						| `Ok (Response.ReduceResult res) -> print_string "b";
							failwith "Unexpected reduce result."
						| `Ok (Response.JobFailed str) -> print_string "c"; failwith str
						| `Eof -> print_string "d"; failwith "Connection to worker closed unexpectedly."
					)
				)
		(* combine phase *)
		>>| List.flatten
		>>| C.combine
		(* reduce phase: send inters to workers *)
		>>= Deferred.List.map ~how:`Parallel
			~f:(fun (key, inter) -> AQueue.pop queue >>=
				(fun (r, w) ->
					Request.send w (Request.ReduceRequest (key, inter));
					Response.receive r >>=
					(fun result ->
						match result with
						| `Ok (Response.ReduceResult res) ->
							AQueue.push queue (r, w); return (key, res)
						| `Ok (Response.MapResult res) ->
							failwith "Unexpected reduce result."
						| `Ok (Response.JobFailed str) -> failwith str
						| `Eof -> failwith "Connection to worker closed unexpectedly."
					)
				)
			)
end


