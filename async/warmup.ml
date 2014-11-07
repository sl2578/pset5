open Async.Std

let fork (d: 'a Deferred.t) (f1: 'a -> 'b Deferred.t) (f2: 'a -> 'c Deferred.t) : unit =
	ignore(d >>= (fun x -> Deferred.both (f1 x) (f2 x)))


(* Need to take a list of 'a and convert to deferred type *)
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