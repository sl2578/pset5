open Async.Std

let fork (d: 'a Deferred.t) (f1: 'a -> 'b Deferred.t) (f2: 'a -> 'c Deferred.t) : unit =
	ignore(d >>= (fun x -> Deferred.both (f1 x) (f2 x)))


(* Need to take a list of 'a and convert to deferred type *)
let deferred_map (l: 'a list) (f: 'a -> 'b Deferred.t) : 'b list Deferred.t = failwith "dsds"
(*   List.fold_right
  	(fun x acc ->
  		(f x) >>= (fun h ->
  		acc >>= (fun t ->
  		Deferred.return (h::t))))
		l (Deferred.return []) *)