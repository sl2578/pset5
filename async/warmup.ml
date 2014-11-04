open Async.Std

let fork (d: 'a Deferred.t) (f1: 'a -> 'b Deferred.t) (f2: 'a -> 'c Deferred.t) : unit =
	Deferred.don't_wait_for (
		Deferred.both
		(Deferred.bind d f1)
		(Deferred.bind d f2); return ()
	)


let deferred_map (l: 'a list) (f: 'a -> 'b Deferred.t) : 'b list Deferred.t =
  failwith "Ditto."
