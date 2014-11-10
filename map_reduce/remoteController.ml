open Async.Std

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


 (*  let reduce (k, vs) =
    Job.reduce (k, vs) >>| fun out -> (k, out)

  module C = Combiner.Make(Job)*)

  let deferred_map (l: 'a list) (f: 'a -> 'b Deferred.t) : 'b list Deferred.t =
	(* convert 'a list to 'b deferred list *)
	let list_of_deferred = 
		List.fold_right (fun acc x -> (f x)::acc) l [] in 
	(* convert 'b deferred list to 'b list differed *)
	List.fold_right 
	(fun x acc -> 
		x >>= (fun h -> 
		acc >>= (fun t -> 
		Deferred.return (h::t)))) 
		list_of_deferred (Deferred.return [])

  let map_reduce inputs = 
  	(deferred_map (!addr) 
  	(fun x -> match x with 
  	| (host, port) -> 
  		try Tcp.connect(Tcp.to_host_and_port host port)) 
  		with InfrastructureFailure -> "cannot make connection to port: %i" port)
  )
    (* deferred_map inputs Job.map
    >>| List.flatten
    >>| C.combine
    >>= deferred_map inputs reduce *)
end


