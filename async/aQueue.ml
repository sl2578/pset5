open Async.Std

type 'a t = 'a Pipe.Reader.t * 'a Pipe.Writer.t

let create () = Pipe.create ()

let push q x = 
	match q with
	| (r, w) -> Pipe.write_without_pushback w x

let pop q = match q with
	| (r, w) -> Pipe.read r >>= function 
	  	| `Ok re -> print_string "worker popped"; return re
	  	| `Eof -> failwith "Failed to pop onto Queue"
