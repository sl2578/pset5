open Async.Std

let fork (d: 'a Deferred.t) (f1: 'a -> 'b Deferred.t) (f2: 'a -> 'c Deferred.t) : unit =
	ignore(d >>= (fun x -> Deferred.both))


let deferred_map (l: 'a list) (f: 'a -> 'b Deferred.t) : 'b list Deferred.t =
  failwith "Ditto."
