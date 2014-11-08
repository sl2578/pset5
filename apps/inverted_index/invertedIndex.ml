open Async.Std
open Async_unix

type filename = string

(******************************************************************************)
(** {2 The Inverted Index Job}                                                *)
(******************************************************************************)

module Job = struct
  type doc = string
  type content = string
  type word = string

  type input = doc * content
  type key = word
  type inter = doc
  type output = doc list

  let name = "index.job"

  (* parse each document content, emit (word, doc) pairs *)
  let map (input : input) : (key * inter) list Deferred.t =
    match input with
    | (doc, content) ->
      let words = AppUtils.split_words content in
      return (List.fold_left (fun acc word -> (word, doc)::acc) [] words)

  (* return list of docs *)
  let reduce (_, inters) : output Deferred.t =
    return inters
end

(* register the job *)
let () = MapReduce.register_job (module Job)


(******************************************************************************)
(** {2 The Inverted Index App}                                                *)
(******************************************************************************)

module App  = struct

  let name = "index"

  (** Print out all of the documents associated with each word *)
  let output results =
    let print (word, documents) =
      print_endline (word^":");
      List.iter (fun doc -> print_endline ("    "^doc)) documents
    in

    let sorted = List.sort compare results in
    List.iter print sorted


  (** for each line f in the master list, output a pair containing the filename
      f and the contents of the file named by f.  *)
  let read (master_file : filename) : (filename * string) list Deferred.t =
    Reader.file_lines master_file >>= fun filenames ->

    Deferred.List.map filenames (fun filename ->
      Reader.file_contents filename >>| fun contents ->
      (filename, contents)
    )

  module Make (Controller : MapReduce.Controller) = struct
    module MR = Controller(Job)

    (** The input should be a single file name.  The named file should contain
        a list of files to index. *)
    let main args =
      if (List.length args) <> 1 then failwith "Please provide one master file."
      else read (List.hd args)
        >>= MR.map_reduce
        >>| output
  end
end

(* register the App *)
let () = MapReduce.register_app (module App)

