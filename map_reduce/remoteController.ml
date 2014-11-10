open Async.Std
open AQueue

let addr = ref []

let init addrs = addr := addrs; ()

exception InfrastructureFailure
exception MapFailure of string
exception ReduceFailure of string

module Make (Job : MapReduce.Job) = struct
  (* Remember that map_reduce should be parallelized. Note that [Deferred.List]
     functions are run sequentially by default. To achieve parallelism, you may
     find the data structures and functions you implemented in the warmup
     useful. Or, you can pass [~how:`Parallel] as an argument to the
     [Deferred.List] functions.*)

	let deferred_map (l: 'a list) (f: 'a -> 'b Deferred.t) : 'b list Deferred.t =	
		(* convert 'a list to 'b Deferred.t list after applying f *)
		let list_of_deferred = List.fold_left (fun acc x -> (f x)::acc) [] l in
			(* change each 'b Deferred.t in list to 'b *)
			List.fold_left(fun acc d ->
				d >>= (fun x ->
				acc >>= (fun xs ->
				return (x::xs))))
				(return []) list_of_deferred

  let map_reduce inputs =
  	(* connect to each worker *)
  	let workers = Deferred.List.map ~how:`Parallel !addr ~f:
	  	(fun x -> match x with 
	  		| (host, port) ->
	  			try Tcp.connect(Tcp.to_host_and_port host port)
	  			with InfrastructureFailure -> failwith "cannot make connection"
	  	) in
	  (* push each worker onto queue *)
	  let queue = Deferred.List.fold workers (AQueue.create ())
	  						(fun q w-> AQueue.push q w) in


    (* deferred_map inputs Job.map
    >>| List.flatten
    >>| C.combine
    >>= deferred_map inputs reduce *)
end


