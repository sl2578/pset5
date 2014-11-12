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
  	let queue = AQueue.create () in
  	Deferred.List.iter ~how:`Parallel !addr
  	~f:(fun (host, port) ->
  		(* get socket and push onto queue *)
  		Tcp.connect (Tcp.to_host_and_port host port) >>=
  		(fun worker -> return (AQueue.push queue worker)));

(*   	let workers = Deferred.List.map ~how:`Parallel !addr
  		~f:(fun (host, port) ->
  			Tcp.connect (Tcp.to_host_and_port host port)) in
  	let queue = AQueue.create () in
		Deferred.List.iter ~how:`Parallel workers
			~f:(fun w -> AQueue.push queue w); *)

		(* map phase: send input to workers *)
		Deferred.List.map ~how:`Parallel inputs
		~f:(fun input -> AQueue.pop queue >>=
			(* send a MapRequest to worker *)
			(fun (sock, r, w) ->
				Request.send w (Request.MapRequest input);
				(* receive worker result and push it back onto queue*)
				Response.receive r >>=
				(fun result ->
					match result with
					| `Ok r -> AQueue.push queue (sock, r, w); r
					| `Eof -> AQueue.push queue (sock, r, w)
				)
			)
		)
		(* combine phase *)
		>>| List.flatten
		>>| C.combine
		(* reduce phase *)
		>>= Deferred.List.map ~how:`Parallel inputs
		~f:(fun inter -> AQueue.pop queue >>= 
			(fun (sock, r, w) ->
				Request.send w (ReduceRequest (k, inter));
				(* receive worker result *)
				let res = Response.receive r in
				AQueue.push queue (sock, r, w); return (k, res))
		)
end


