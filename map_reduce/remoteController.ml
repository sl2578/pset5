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
  	(* queue for holding workers *)
  	let queue = AQueue.create () in
  	(* iterate through addresses *)
  	Deferred.List.iter ~how:`Parallel !addr
  	~f:(fun (host, port) ->
  		(* connect to each worker uses addresses *)
  		let worker = try (Tcp.connect(Tcp.to_host_and_port host port)) with
  		| _ -> failwith "AHHHH you fucked up man" in
  		(* push worker onto queue *)
	  	return(AQueue.push queue worker));

(*   	let workers = Deferred.List.map ~how:`Parallel !addr ~f:
	  	(fun x -> match x with 
	  		| (host, port) ->
	  			try (Tcp.connect(Tcp.to_host_and_port host port)) with
	  			| _ -> failwith "AHHHH you fucked up man") in
	  (* push each worker onto queue *)
		let queue = AQueue.create () in 
			Deferred.List.iter ~how:`Parallel workers
			~f:(fun w -> AQueue.push queue w); *)
		(* map phase, send input to workers *)
		Deferred.List.map ~how:`Parallel inputs
		~f:(fun input -> AQueue.pop queue >>=
				(* send input to worker through its writer *)
				(fun (sock, r, w) -> Request.send w input;
					(* receive inter from worker *)
					let res = Response.receive r in
					(* push worker back to queue *)
					AQueue.push queue (sock, r, w); res)
		)
		(* combine phase *)
		>>| List.flatten
		>>| C.combine
		(* reduce phase *)
		>>= Deferred.List.map ~how:`Parallel
		~f:(fun inter -> 
			(* pop worker from queue *)
			AQueue.pop queue >>= 
				(* 	 *)
				(fun (sock, r, w) -> Request.send w (k, inter);
					let res = Response.recieve r in
					AQueue.push queue (sock, r, w); return (k, res))
		)
end


