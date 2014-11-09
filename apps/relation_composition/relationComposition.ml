open Async.Std
open AppUtils

type relation = R | S

module Job = struct  
  type input  = relation * string * string
  type key    = string 
  type inter  = relation * string
  type output = (string * string) list

  let name = "composition.job"

  let map (input : input) =
    match input with
    | (R, x, y) -> return ([y, (R, x)])
    | (S, x, y) -> return ([x, (S, y)])

  let reduce ((k : key), (vs : inter list)) = (* currently takes O(n^2) where n = # values *)
    (* for given pair, add all relations found with rest of values *)
    let find_relation acc (r, v) =
      match r with
      | R ->
        List.fold_left
          (fun acc (r2, v2) ->
            match r2 with
            | S -> (v, v2)::acc
            | R -> acc)
          acc vs
      | S -> acc in
    return (List.fold_left find_relation [] vs)
end

let () = MapReduce.register_job (module Job)

let read_line (line: string) : (string * string) =
  match Str.split (Str.regexp ",") line with
    | [domain; range] -> (String.trim domain, String.trim range)
    | _ -> failwith "Malformed input in relation file."

let read_file (r: relation) (file: string) : (relation * string * string) list Deferred.t =
      Reader.file_lines file            >>= fun lines  ->
      return (List.map read_line lines) >>= fun result ->
      return (List.map (fun (domain, range) -> (r, domain, range)) result)

module App = struct
  let name = "composition"

  let clean_and_print vs =
    List.map snd vs   |>
    List.flatten      |>
    List.sort compare |>
    List.iter (fun (a, b) -> printf "(%s, %s)\n" a b)

  module Make (Controller : MapReduce.Controller) = struct
    module MR = Controller(Job)

    (* You may assume that main is called with two valid relation files. You do
       not need to handle malformed input. For example relation files, see the
       data directory. *)
    let main args =
      match args with
      | [rfile; sfile] -> begin
          read_file R rfile >>= fun r ->
          read_file S sfile >>= fun s ->
          return (r @ s)
          >>= MR.map_reduce
          >>| clean_and_print
      end
      | _ -> failwith "Incorrect number of input files. Please provide two files."
  end
end

let () = MapReduce.register_app (module App)
