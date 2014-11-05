open Async.Std


let fork (d: 'a Deferred.t) (f1: 'a -> 'b Deferred.t) (f2: 'a -> 'c Deferred.t) : unit =
	ignore(d >>= (fun x -> Deferred.both (f1 x) (f2 x)))

let deferred_map (l: 'a list) (f: 'a -> 'b Deferred.t) : 'b list Deferred.t =	
	(* convert 'a list to 'b Deferred.t list after applying f *)
	let list_of_deferred = List.fold_left (fun acc x -> (f x)::acc) [] l in
		(* change each 'b Deferred.t in list to 'b *)
		List.fold_left(fun acc d ->
			d >>= (fun x ->
			acc >>= (fun xs ->
			return (x::xs))))
			(return []) list_of_deferred