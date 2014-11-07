open Async.Std

type 'a t = 'a Pipe.Reader.t * 'a Pipe.Writer.t

let create () = Pipe.create ()

let push q x = 
	match q with
	| (r, w) -> Pipe.write_without_pushback w x


let pop  q =
  match q with
  | (r, w) -> Pipe.read r 
