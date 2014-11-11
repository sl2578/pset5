open Async.Std
open AQueue

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

  let map_reduce inputs =
  	(* connect to each worker *)
  	let workers = Deferred.List.map ~how:`Parallel !addr ~f:
	  	(fun x -> match x with 
	  		| (host, port) ->
	  			match try_with (fun _ -> Tcp.connect(Tcp.to_host_and_port host port)) with
	  			| Core.Std.Error _ -> (*TODO: num_workers := !num_workers - 1; 
	  				if (!num_workers <= 0) 
	  					then raise InfrastructureFailure *) failwith "No workers connected"
	  	) in
	  (* push each worker onto queue *)
	let queue = Deferred.List.fold workers (AQueue.create ())
	  	(fun q w-> AQueue.push q w) in

	(* map phase, send input to workers *)
	Deferred.List.map ~how:'Parallel inputs 
	~f:(fun input ->
		(* pop worker from queue *) 
		AQueue.pop queue >>=
			(* send input to worker through its writer *)
			(fun (sock, r, w) ->  WorkerRequest(Job).send w input; 
				(* receive inter from worker *)
				let res = WorkerResponse(Job).receive r in
				(* push worker back to queue *)
				AQueue.push queue (sock, r w); res)
	)
		(* combine phase *)
		>>| List.flatten
		>>| C.combine
		(* reduce phase *)
		>>= Deferred.List.map ~how:'Parallel ~f:
		(fun (k, inter) -> 
			(* pop worker from queue *)
			AQueue.pop queue >>= 
				(* *)
				(fun (sock, r, w) -> WorkerRequest(Job).send w (k, inter);
					let res = WorkerResponse(Job).recieve r in
					AQueue.push queue (sock, r, w); return (k, res))
		)
end


