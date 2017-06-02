(*******************************************************************************
	This file is a part of x264farm.

	x264farm is free software; you can redistribute it and/or modify
	it under the terms of the GNU General Public License as published by
	the Free Software Foundation; either version 2 of the License, or
	(at your option) any later version.

	x264farm is distributed in the hope that it will be useful,
	but WITHOUT ANY WARRANTY; without even the implied warranty of
	MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
	GNU General Public License for more details.

	You should have received a copy of the GNU General Public License
	along with x264farm; if not, write to the Free Software
	Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301  USA
*******************************************************************************)

open Types;;
open Pass1;;
open Pass2;;
open OptParse;;

let debug_file = true;;

let time = Sys.time ();;

(*perl -e "for $a (1 .. (388800 * 64)) {print chr(rand(255))}" | x264 --verbose -o blimey.mkv - 720x360*)
(*perl -e "for $a (1 .. (388800 * 64)) {print chr($a % 242)}" | x264 --verbose -o blimey.mkv - 720x360*)

let _ = (

(************************************)
(* PARSE THE COMMAND LINE ARGUMENTS *)
(************************************)

let o = (
(*	let help_ref = ref false in*)
(*
	let second_pass_bitrate_ref = ref (Percent 100) in
	let first_option_ref = ref "" in
	let second_option_ref = ref "" in
	let input_avs_ref = ref "" in
	let input_first_avs_ref = ref "" in
	let input_fast_avs_ref = ref "" in
	let output_file_ref = ref "output.mkv" in (* The output name (must be MKV) *)

	let preseek_ref = ref 0 in
	let force_ref = ref false in
	let restart_ref = ref false in
	let config_file_ref = ref "config.xml" in (* Config file location *)

	let optimal_batch_length_ref = ref 5000 in (* How many frames per batch to send out *)
	let split_frame_length_ref = ref 250 in (* How far to parse the file to pick the optimal split point *)
	let split_thresh_ref = ref 20.0 in (* How much larger the most-different frame must be from the average in order to output it *)

	let third_pass_threshold_ref = ref 0.8 in
	let third_pass_max_gops_ref = ref 1073741823 in
	let third_pass_max_gop_ratio_ref = ref 0.05 in
	let rerc_gops_ref = ref 0 in

	let zones_ref = ref [] in
	let seek_ref = ref 0 in
	let frames_ref = ref 0 in (* 0 means normal *)
	let rc_qcomp_ref = ref 0.6 in
	let rc_cplxblur_ref = ref 20.0 in
	let rc_qblur_ref = ref 0.5 in
	let rc_qpmin_ref = ref 10 in
	let rc_qpmax_ref = ref 51 in
	let rc_qpstep_ref = ref 4 in
	let rc_ipratio_ref = ref 1.4 in
	let rc_bpratio_ref = ref 1.3 in

	let input_option_file_ref = ref "" in (* Gives the name of a file containing options *)
*)
	let parse_bitrate x = (
		if debug_file then Printf.printf "Parsing bitrate %s\n" x;
		(if String.length x < 2 then raise (Unsupported_argument "Bitrate needs to be in the form \"123%\" or \"123.4kbps\""));
		if x.[String.length x - 1] = '%' then (
			try
				Percent (int_of_string (String.sub x 0 (String.length x - 1)))
			with
			| Failure "int_of_string" -> raise (Unsupported_argument "Bitrate needs to be in the form \"123%\" or \"123.4kbps\"")
		) else if (String.length x > 4 && String.lowercase (String.sub x (String.length x - 4) 4) = "kbps") then (
			try
				Kbps (float_of_string (String.sub x 0 (String.length x - 4)))
			with
			| Failure "float_of_string" -> raise (Unsupported_argument "Bitrate needs to be in the form \"123%\" or \"123.4kbps\"")
		) else (
			try
				let i = float_of_string x in
				if i > 200.0 then (
					print "WARNING: No units given to bitrate \"%s\"; assuming kbps\n" x;
					Kbps i
				) else (
					print "WARNING: No units given to bitrate \"%s\"; assuming percent\n" x;
					Percent (int_of_float i)
				)
			with
				_ -> raise (Unsupported_argument "Bitrate needs to be in the form \"123%\" or \"123.4kbps\"")
		)
	) in

	let parse_zones str =
		if debug_file then Printf.printf "parse_zones %S\n" str;
		let parse_zone str = (
			if debug_file then Printf.printf "parse_zone %S\n" str;
			try (
				let comma1 = String.index str ',' in
				let comma2 = String.index_from str (succ comma1) ',' in
(*				let equals = String.index_from str (succ comma2) '=' in*)
				let zone_start = int_of_string (String.sub str 0 comma1) in
				let zone_end   = int_of_string (String.sub str (succ comma1) (pred comma2 - comma1)) in
				let number = float_of_string (String.sub str (comma2 + 3) (String.length str - comma2 - 3)) in
				if number <= 0.0 then raise (Invalid_zone str);
				let zone_setup = match String.sub str (succ comma2) 2 with
					| "b=" | "B=" -> Zone_bitrate number
					| "q=" | "Q=" -> Zone_q number
					| x -> (if debug_file then Printf.printf "ERROR %S\n" x; Zone_bitrate 1.0)
				in
				if debug_file then (
					match zone_setup with
					| Zone_bitrate x -> Printf.printf " {%d,%d,b=%f}\n" zone_start zone_end number
					| Zone_q       x -> Printf.printf " {%d,%d,q=%f}\n" zone_start zone_end number
				);
				{zone_start = zone_start; zone_end = zone_end; zone_type = zone_setup}
			) with
			| Not_found | Failure _ -> raise (Invalid_zone str)
		) in
		let rec parse_zones_rec str =
			Printf.printf "parse_zones_rec %S\n" str;
			match String.contains str '/' with
			| false -> parse_zone str :: []
			| true -> (
					let slash_location = String.index str '/' in
					parse_zone (String.sub str 0 slash_location) :: parse_zones_rec (String.sub str (succ slash_location) (String.length str - succ slash_location))
				)
		in
		parse_zones_rec str
	in



	(* PARSE *)
(*
	let arg_parse = Arg.align [
		("-b",        Arg.String parse_bitrate,                   " The second pass bitrate (in % or kbps) Example: -b 800kbps");
		("--first",   Arg.Set_string first_option_ref,            " First pass settings (leave out filenames and pass information)");
		("--second",  Arg.Set_string second_option_ref,           " Second pass settings (leave out input, output, and bitrate");
		("--avs",     Arg.Set_string input_avs_ref,               " Input AVS file to use");
		("-i",        Arg.Set_string input_avs_ref,               " Same as --avs");
		("--firstavs",Arg.Set_string input_first_avs_ref,         " AVS file to use for the first pass (defaults to --avs)");
		("--fastavs", Arg.Set_string input_fast_avs_ref,          " Input AVS file for frame testing");
		("-o",        Arg.Set_string output_file_ref,             " Output file name");
		("--batch",   Arg.Set_int optimal_batch_length_ref,       " How many frames to give out at a time (5000)");
		("--split",   Arg.Set_int split_frame_length_ref,         " How many frames to check for the optimal frame change (250)");
		("--thresh",  Arg.Set_float split_thresh_ref,             " The threshold for early-abort of the frame change (10.0)");
		("--preseek", Arg.Set_int preseek_ref,                    " Frames to seek to before the requested one (0)");
		("--force",   Arg.Set force_ref,                          " Forces the encoding, even if it had been done before");
		("--restart", Arg.Set restart_ref,                        " Deletes all previous data and starts over");
		("--config",  Arg.Set_string config_file_ref,             " Location of the config XML file (\"config.xml\")");
		("--3thresh", Arg.Set_float third_pass_threshold_ref,     " Threshold for 3rd pass. Ratio of 2nd_pass/optimal bitrates (0.8)");
		("--3gops",   Arg.Set_int third_pass_max_gops_ref,        " Max GOPs to process for 3rd pass. 0 will skip 3rd pass (large)");
		("--3ratio",  Arg.Set_float third_pass_max_gop_ratio_ref, " Max GOPs. 0.05 -> 5% of the total GOPs will be redone (0.05)");
		("--rerc",    Arg.Set_int ratecontrol_gops_ref,           " Max GOPs in the 3rd pass before ratecontrol is redone (0)");
		("--zones",   Arg.String parse_zones,                     " Same as x264's --zones");
		("--seek",    Arg.Set_int seek_ref,                       " Same as x264's --seek");
		("--frames",  Arg.Set_int frames_ref,                     " Same as x264's --frames");
		("--qcomp",   Arg.Set_float rc_qcomp_ref,                 " Same as x264's --qcomp (0.6)");
		("--cplxblur",Arg.Set_float rc_cplxblur_ref,              " Same as x264's --cplxblur (20.0)");
		("--qblur",   Arg.Set_float rc_qblur_ref,                 " Same as x264's --qblur (0.5)");
		("--qpmin",   Arg.Set_int rc_qpmin_ref,                   " Same as x264's --qpmin (10)");
		("--qpmax",   Arg.Set_int rc_qpmax_ref,                   " Same as x264's --qpmax (51)");
		("--qpstep",  Arg.Set_int rc_qpstep_ref,                  " Same as x264's --qpstep (4)");
		("--ipratio", Arg.Set_float rc_ipratio_ref,               " Same as x264's --ipratio (1.4)");
		("--bpratio", Arg.Set_float rc_bpratio_ref,               " Same as x264's --bpratio (1.3)");
	] in
	Arg.parse arg_parse (fun x -> input_option_file_ref := x) (Printf.sprintf "Distributed encoding (version %s)" version);
*)
	let p = OptParser.make ~suppress_help:false () in

	let opt_bitrate  = StdOpt.str_option   ~default:"100%"       ~metavar:"" () in
	let opt_first    = StdOpt.str_option   ~default:""           ~metavar:"" () in
	let opt_second   = StdOpt.str_option   ~default:""           ~metavar:"" () in
	let opt_avs      = StdOpt.str_option   ~default:""           ~metavar:"" () in
	let opt_firstavs = StdOpt.str_option   ~default:""           ~metavar:"" () in
	let opt_fastavs  = StdOpt.str_option   ~default:""           ~metavar:"" () in
	let opt_output   = StdOpt.str_option   ~default:"output.mkv" ~metavar:"" () in

	let opt_preseek  = StdOpt.int_option   ~default:0            ~metavar:"" () in
	let opt_force    = StdOpt.store_true                         () in
	let opt_restart  = StdOpt.store_true                         () in
	let opt_config   = StdOpt.str_option   ~default:"config.xml" ~metavar:"" () in
	let opt_savedisk = StdOpt.store_true                         () in
	let opt_nocomp   = StdOpt.store_true                         () in

	let opt_batch    = StdOpt.int_option   ~default:5000         ~metavar:"" () in
	let opt_split    = StdOpt.int_option   ~default:250          ~metavar:"" () in
	let opt_thresh   = StdOpt.float_option ~default:20.0         ~metavar:"" () in

	let opt_3thresh  = StdOpt.float_option ~default:0.8          ~metavar:"" () in
	let opt_3gops    = StdOpt.int_option   ~default:1073741823   ~metavar:"" () in
	let opt_3ratio   = StdOpt.float_option ~default:0.05         ~metavar:"" () in
	let opt_rerc     = StdOpt.int_option   ~default:0            ~metavar:"" () in

	let opt_zones    = StdOpt.str_option   ~default:""           ~metavar:"" () in
	let opt_seek     = StdOpt.int_option   ~default:0            ~metavar:"" () in
	let opt_frames   = StdOpt.int_option   ~default:0            ~metavar:"" () in
	let opt_qcomp    = StdOpt.float_option ~default:0.6          ~metavar:"" () in
	let opt_cplxblur = StdOpt.float_option ~default:20.0         ~metavar:"" () in
	let opt_qblur    = StdOpt.float_option ~default:0.5          ~metavar:"" () in
	let opt_qpmin    = StdOpt.int_option   ~default:10           ~metavar:"" () in
	let opt_qpmax    = StdOpt.int_option   ~default:51           ~metavar:"" () in
	let opt_qpstep   = StdOpt.int_option   ~default:4            ~metavar:"" () in
	let opt_ipratio  = StdOpt.float_option ~default:1.4          ~metavar:"" () in
	let opt_pbratio  = StdOpt.float_option ~default:1.3          ~metavar:"" () in

	let group_general = OptParser.add_group p "General x264 parameters" in
(*	OptParser.add p ~group:group_general ~short_name:'h' ~long_name:"help"     ~help:"HEYELP!" opt_help;*)
	OptParser.add p ~group:group_general ~short_name:'B' ~long_name:"bitrate"  ~help:(Printf.sprintf "Second pass bitrate; may be in %% or kbps, as in \"-B 400kbps\" (%s)" (Opt.get opt_bitrate)) opt_bitrate;
	OptParser.add p ~group:group_general ~short_name:'1' ~long_name:"first"    ~help:"First pass setting string (\"\")" opt_first;
	OptParser.add p ~group:group_general ~short_name:'2' ~long_name:"second"   ~help:"Second pass setting string (\"\")" opt_second;
	OptParser.add p ~group:group_general ~short_name:'i' ~long_name:"avs"      ~help:"The input AVS file (\"\")" opt_avs;
	OptParser.add p ~group:group_general                 ~long_name:"firstavs" ~help:"The AVS file to use for the first pass. Defaults to whatever --avs is set to" opt_firstavs;
	OptParser.add p ~group:group_general                 ~long_name:"fastavs"  ~help:"Input AVS for first-pass split testing. Defaults to whatever --firstavs is set to" opt_fastavs;
	OptParser.add p ~group:group_general ~short_name:'o' ~long_name:"output"   ~help:(Printf.sprintf "Output MKV file (%S)" (Opt.get opt_output)) opt_output;

	let group_control = OptParser.add_group p "x264farm control parameters" in
	OptParser.add p ~group:group_control                 ~long_name:"preseek"       ~help:(Printf.sprintf "Number of frames to render before each job start (%d)" (Opt.get opt_preseek)) opt_preseek;
	OptParser.add p ~group:group_control                 ~long_name:"force"         ~help:"Resume encode even if it was already finished" opt_force;
	OptParser.add p ~group:group_control                 ~long_name:"restart"       ~help:"Remove all temp files and start over" opt_restart;
	OptParser.add p ~group:group_control                 ~long_name:"config"        ~help:(Printf.sprintf "File for agent configuration settings (%S)" (Opt.get opt_config)) opt_config;
	OptParser.add p ~group:group_control                 ~long_name:"savedisk"      ~help:"Lowers the max disk space usage by deleting files as soon as they are concatenated" opt_savedisk;
	OptParser.add p ~group:group_control                 ~long_name:"nocomp"        ~help:"Turns off controller-based compression, even if agents request it" opt_nocomp;

	let group_1st = OptParser.add_group p "First pass split parameters" in
	OptParser.add p ~group:group_1st                     ~long_name:"batch"  ~help:(Printf.sprintf "How many frames to give out at a time (%d)" (Opt.get opt_batch)) opt_batch;
	OptParser.add p ~group:group_1st                     ~long_name:"split"  ~help:(Printf.sprintf "How many frames to check for optimal frame length (%d)" (Opt.get opt_split)) opt_split;
	OptParser.add p ~group:group_1st                     ~long_name:"thresh" ~help:(Printf.sprintf "Threshold for early-abort of the frame change (%F)" (Opt.get opt_thresh)) opt_thresh;

	let group_2nd = OptParser.add_group p "Selective third-pass parameters" in
	OptParser.add p ~group:group_2nd                     ~long_name:"3thresh" ~help:(Printf.sprintf "Max accuracy to consider for 3rd pass (0..1) (%F)" (Opt.get opt_3thresh)) opt_3thresh;
	OptParser.add p ~group:group_2nd                     ~long_name:"3gops"   ~help:(Printf.sprintf "Max number of GOPs to consider for 3rd pass (%d)" (Opt.get opt_3gops)) opt_3gops;
	OptParser.add p ~group:group_2nd                     ~long_name:"3ratio"  ~help:(Printf.sprintf "Max percentage of GOPs to consider for 3rd pass (%F)" (Opt.get opt_3ratio)) opt_3ratio;
	OptParser.add p ~group:group_2nd                     ~long_name:"rerc"    ~help:(Printf.sprintf "Number of GOPs between RC passes (%d)" (Opt.get opt_rerc)) opt_rerc;

	let group_x264 = OptParser.add_group p "These are the same as x264 and should be used outside the -1 and -2 strings" in
	OptParser.add p ~group:group_x264                    ~long_name:"zones"    ~help:(Printf.sprintf "(\"%s\")" (Opt.get opt_zones)) opt_zones;
	OptParser.add p ~group:group_x264                    ~long_name:"seek"     ~help:(Printf.sprintf "(%d)" (Opt.get opt_seek)) opt_seek;
	OptParser.add p ~group:group_x264                    ~long_name:"frames"   ~help:(Printf.sprintf "(%d)" (Opt.get opt_frames)) opt_frames;
	OptParser.add p ~group:group_x264                    ~long_name:"qcomp"    ~help:(Printf.sprintf "(%F)" (Opt.get opt_qcomp)) opt_qcomp;
	OptParser.add p ~group:group_x264                    ~long_name:"cplxblur" ~help:(Printf.sprintf "(%F)" (Opt.get opt_cplxblur)) opt_cplxblur;
	OptParser.add p ~group:group_x264                    ~long_name:"qblur"    ~help:(Printf.sprintf "(%F)" (Opt.get opt_qblur)) opt_qblur;
	OptParser.add p ~group:group_x264                    ~long_name:"qpmin"    ~help:(Printf.sprintf "(%d)" (Opt.get opt_qpmin)) opt_qpmin;
	OptParser.add p ~group:group_x264                    ~long_name:"qpmax"    ~help:(Printf.sprintf "(%d)" (Opt.get opt_qpmax)) opt_qpmax;
	OptParser.add p ~group:group_x264                    ~long_name:"qpstep"   ~help:(Printf.sprintf "(%d)" (Opt.get opt_qpstep)) opt_qpstep;
	OptParser.add p ~group:group_x264                    ~long_name:"ipratio"  ~help:(Printf.sprintf "(%F)" (Opt.get opt_ipratio)) opt_ipratio;
	OptParser.add p ~group:group_x264                    ~long_name:"pbratio"  ~help:(Printf.sprintf "(%F)" (Opt.get opt_pbratio)) opt_pbratio;

	let others = OptParser.parse_argv p in

	(* Read the file specified on the command line (if any) *)
	let apply_arguments_from_file input_option_file = if Sys.file_exists input_option_file then (
		if debug_file then Printf.printf "Read stuff from %S\n" input_option_file;
		try (
			let option_handle = open_in input_option_file in
			let rec add_line () = (
				try (
					match input_line option_handle with
					| "" -> (if debug_file then Printf.printf " (blank line)\n"; add_line ())
					| x when x.[0] = '#' -> (if debug_file then Printf.printf " Ignored %S\n" x; add_line ())
					| x -> x :: add_line ()
				) with
				| End_of_file -> []
			) in
			let options = Array.of_list ((Sys.argv.(0)) :: (add_line ())) in
			if debug_file then Array.iter (fun x -> Printf.printf "  %S\n" x) options;

			close_in option_handle;

(*			Getopt.parse parse_this ignore options 1 (Array.length options - 1);*)
			(try
				ignore (OptParser.parse p options)
			with
				x -> ()
			);
(*
			let ref_zero = ref 0 in
			Arg.parse_argv ~current:ref_zero options arg_parse (fun x -> ()) (Printf.sprintf "Distributed encoding (version %s, called from a file)" version);
*)
			if debug_file then Array.iter (fun x -> Printf.printf "  %S\n" x) options;
			true
		) with
		| x -> (print "\nOOPS! %S\n" (Printexc.to_string x); false)
	) else (
		false
	) in

	List.iter (fun x -> if (apply_arguments_from_file x) then () else (print "Oops! File \"%s\" does not exist\n" x)) others;

	let input_avs = if is_file (Opt.get opt_avs) then (Opt.get opt_avs) else (OptParser.usage p (); print "ERROR: input AVS \"%s\" is not a file\n" (Opt.get opt_avs); exit 1) in
	let input_first_avs = if is_file (Opt.get opt_firstavs) then (Opt.get opt_firstavs) else input_avs in
	let input_fast_avs = if is_file (Opt.get opt_fastavs) then (Opt.get opt_fastavs) else input_first_avs in

	let first_option = Opt.get opt_first in
	if first_option = "" then print "WARNING: First pass options are not set; this will use the default x264 settings\n";
	let second_option = Opt.get opt_second in
	if second_option = "" then print "WARNING: Second pass options are not set; this will use the default x264 settings\n";

	let config_file = match (search_for_file (Opt.get opt_config), search_for_file "config.xml") with
		| (Some x, _) -> x
		| (None, Some y) -> (print "WARNING: Config file \"%s\" does not exist; using \"%s\" instead\n" (Opt.get opt_config) y; y)
		| (None, None) -> (OptParser.usage p (); print "ERROR: No configuration file found; exiting\n"; exit 1)
	in
	print "Using config file \"%s\"\n" config_file;

	let output_file = (
		let got = Opt.get opt_output in
		let temp_output_file = (
			if String.length got >= 4 && String.lowercase (String.sub got (String.length got - 4) 4) = ".mkv" then (
				got
			) else (
				let mkv = got ^ ".mkv" in
				print "WARNING: Given file name \"%s\" is not a .mkv file; outputting to \"%s\" instead\n" got mkv;
				mkv
			)
		) in
		(try
			match (Unix.LargeFile.stat temp_output_file).Unix.LargeFile.st_kind with
			| Unix.S_REG -> (print "WARNING: Output file '%s' already exists and will be overwritten!\n" temp_output_file; flush stderr; temp_output_file)
			| _ -> (OptParser.usage p (); print "ERROR: Output file '%s' is not a file\n" temp_output_file; exit 1)
		with
			Unix.Unix_error(Unix.ENOENT, "stat", _) -> (temp_output_file)
		)
	) in

	(*************************)
	(* PARSE THE CONFIG FILE *)
	(*************************)
	let temp_dir_ref = ref "" in
	let agent_list_ref = ref [] in

	let config_xml = Xml.parse_file config_file in

	let parse_agent = function
		| Xml.Element ("agent", [("name", name)], agent_guts) -> (
			let ip_ref = ref "127.0.0.1" in
			let port_from_ref = ref 50700 in
			let port_to_ref = ref 50710 in
			let number_ref = ref 1 in
			let add_two_ref = ref 0 in
			let parse_agent_attribute = function
				| Xml.Element ("ip",    _, [Xml.PCData x]) -> ip_ref := x
				| Xml.Element ("port",  [("from",x); ("to",y)], []) -> (port_from_ref := int_of_string x; port_to_ref := int_of_string y)
				| Xml.Element ("number",[("pad",y)], [Xml.PCData x]) -> (number_ref := int_of_string x; add_two_ref := int_of_string y)
				| Xml.Element ("number",_, [Xml.PCData x]) -> number_ref := int_of_string x
				| _ -> ()
			in
			List.iter parse_agent_attribute agent_guts;
			(name, (!ip_ref, !port_from_ref, !port_to_ref), (!number_ref, !number_ref + !add_two_ref))
		)
		| _ -> ("",("",0,0),(0,0))
	in

	let parse_config = function
		| Xml.Element ("temp", _, [Xml.PCData x]) -> temp_dir_ref := x
		| Xml.Element ("agents", _, y) -> agent_list_ref := List.rev_map parse_agent y
		| _ -> ()
	in

	let parse_root = function
		| Xml.Element ("config", _, x) -> List.iter parse_config x
		| _ -> raise (Invalid_config "root element of config file must be <config>")
	in

	parse_root config_xml;

(*List.iter (fun (name, (ip,pf,pt), (n1,n2)) -> print "\"%s\" = %d,%d\n" name n1 n2) !agent_list_ref;*)

	(* Expands the agents based on the multiplicity (like [(a,3),(b,2)] -> [a,a,a,b,b]) *)

	let rec expand_agents_1 to_do already_done = (
		match to_do with
		| [] -> already_done
		| (_, _, (0,_)) :: tl -> (
			expand_agents_1 tl already_done
		)
		| (name, (ip, pf, pt), (n,_)) :: tl -> (
			expand_agents_1 ((name, (ip, pf, pt), (pred n,0)) :: tl) ((name, (ip, pf, pt), n) :: already_done)
		)
	) in
	let rec expand_agents_2 to_do already_done = (
		match to_do with
		| [] -> already_done
		| (_, _, (_,0)) :: tl -> (
			expand_agents_2 tl already_done
		)
		| (name, (ip, pf, pt), (_,n)) :: tl -> (
			expand_agents_2 ((name, (ip, pf, pt), (0,pred n)) :: tl) ((name, (ip, pf, pt), n) :: already_done)
		)
	) in

	let agents_1 = expand_agents_1 !agent_list_ref [] in
	let agents_2 = expand_agents_2 !agent_list_ref [] in

	(* Again with the valid stuff check *)
	let temp_dir_base = try (
		match (Unix.LargeFile.stat !temp_dir_ref).Unix.LargeFile.st_kind with
		| Unix.S_DIR -> !temp_dir_ref
		| _ -> (print "\nERROR: Temp directory '%s' not a directory; check config.xml\n" !temp_dir_ref; exit 1)
	) with
		Unix.Unix_error(Unix.ENOENT, "stat", _) -> (print "\nERROR: Temp directory '%s' does not exist; check config.xml\n" !temp_dir_ref; exit 1)
	in

	if debug_file then (
		print "CONFIG:\n";
		print " temp_dir: %S\n" temp_dir_base;
		print " agent_list 1:\n";
		List.iter (fun (name, (ip, pf, pt), n) -> print "  \"%s %d\" = %s %d-%d\n" name n ip pf pt) agents_1;
		print " agent_list 2:\n";
		List.iter (fun (name, (ip, pf, pt), n) -> print "  \"%s %d\" = %s %d-%d\n" name n ip pf pt) agents_2;
	);
(*
	print "toast\n";
	exit 4;
*)
	{
		o_first = first_option;
		o_second = second_option;
		o_zones = (if (Opt.get opt_zones) = "" then [] else parse_zones (Opt.get opt_zones));
		o_seek = Opt.get opt_seek;
		o_frames = Opt.get opt_frames;
		o_batch_length = Opt.get opt_batch;
		o_split_frame_length = Opt.get opt_split;
		o_split_thresh = Opt.get opt_thresh;
		o_output_file = output_file;
		o_input_avs = input_avs;
		o_input_avs_full = if Filename.is_relative input_avs then (Filename.concat (Sys.getcwd ()) input_avs) else input_avs;
		o_input_avs_md5 = Digest.file input_avs;
		o_input_fast_avs = input_fast_avs;
		o_input_first_avs = input_first_avs;
		o_input_first_avs_full = if Filename.is_relative input_first_avs then (Filename.concat (Sys.getcwd ()) input_first_avs) else input_first_avs;
		o_input_first_avs_md5 = Digest.file input_first_avs;
		o_second_pass_bitrate = parse_bitrate (Opt.get opt_bitrate);
		o_third_pass_threshold = Opt.get opt_3thresh;
		o_third_pass_max_gops = Opt.get opt_3gops;
		o_third_pass_max_gop_ratio = Opt.get opt_3ratio;
		o_ratecontrol_gops = Opt.get opt_rerc;
		o_temp_dir = temp_dir_base;
		o_preseek = Opt.get opt_preseek;
		o_force = Opt.get opt_force;
		o_restart = Opt.get opt_restart;
		o_savedisk = Opt.get opt_savedisk;
		o_nocomp = Opt.get opt_nocomp;
		o_agents_1 = Array.of_list agents_1;
		o_agents_2 = Array.of_list agents_2;

		o_rc_qcomp =    Opt.get opt_qcomp;
		o_rc_cplxblur = Opt.get opt_cplxblur;
		o_rc_qblur =    Opt.get opt_qblur;
		o_rc_qpmin =    Opt.get opt_qpmin;
		o_rc_qpmax =    Opt.get opt_qpmax;
		o_rc_qpstep =   Opt.get opt_qpstep;
		o_rc_ipratio =  Opt.get opt_ipratio;
		o_rc_pbratio =  Opt.get opt_pbratio;
	}
) in

(*********************************)
(* FIND OUT STUFF ABOUT THE FILE *)
(*********************************)
let i = (

	let match_this_string = (
		let rx_rat = Str.regexp ".*: \\([0-9]+\\)x\\([0-9]+\\), \\([0-9]+\\)/\\([0-9]+\\) fps, \\([0-9]+\\) frames" in
		let rx_int = Str.regexp ".*: \\([0-9]+\\)x\\([0-9]+\\), \\([0-9]+\\) fps, \\([0-9]+\\) frames" in
		fun file_info -> if Str.string_match rx_rat file_info 0 then (
			Some (
				int_of_string (Str.matched_group 1 file_info),
				int_of_string (Str.matched_group 2 file_info),
				int_of_string (Str.matched_group 3 file_info),
				int_of_string (Str.matched_group 4 file_info),
				int_of_string (Str.matched_group 5 file_info),
				float_of_string (Str.matched_group 3 file_info) /. float_of_string (Str.matched_group 4 file_info)
			)
		) else if Str.string_match rx_int file_info 0 then (
			Some (
				int_of_string (Str.matched_group 1 file_info),
				int_of_string (Str.matched_group 2 file_info),
				int_of_string (Str.matched_group 3 file_info),
				1,
				int_of_string (Str.matched_group 4 file_info),
				float_of_string (Str.matched_group 3 file_info)
			)
		) else (
			None
		)
	) in

	(* Find avs2yuv, if it's in the controller's directory *)
	let avs2yuv = (
		match (search_for_file "avs2yuv.exe", search_for_file "avs2yuv.bat") with
		| (Some x, _) -> x
		| (None, Some x) -> x
		| (None, None) -> "avs2yuv" (* Let's hope it's on the path somewhere *)
	) in


	(* 2nd pass AVS *)

	(* This needs all sorts of extra quotation marks because the path to avs2yuv may have spaces in it, and cmd removes the first and last quotation mark *)
	let open_me = Printf.sprintf "\"\"%s\" -frames 1 -raw -o %s \"%s\" 2>&1\"" avs2yuv dev_null o.o_input_avs in
(*
	print "DEBUG: Running command %s\n" open_me;
	print "DEBUG: avs2yuv.exe in current directory (%s)? %B\n" (Sys.getcwd ()) (is_file "avs2yuv.exe");
	(try
		print "DEBUG: opening avs2yuv\n";
		let file_info_handle = Unix.open_process_in (open_me) in (* dev_null is defined in common/types.ml *)
		let out_string = String.create 4096 in
		let read_bytes = input file_info_handle out_string 0 4096 in
		print "DEBUG: avs2yuv responded: %S\n" (String.sub out_string 0 read_bytes);
		(match Unix.close_process_in file_info_handle with
			| Unix.WEXITED 1 -> (print "DEBUG: avs2yuv exited with error 1; AVS file \"%s\" probably has an error\n" o.o_input_avs)
			| Unix.WEXITED x -> (if x <> 0 then (print "\nERROR: avs2yuv exited with error %d\n" x))
			| Unix.WSIGNALED x -> print "DEBUG: avs2yuv was killed with signal %d\n" x
			| Unix.WSTOPPED x -> print "DEBUG: avs2yuv was stopped by signal %d\n" x
		);
	with
		_ -> ()
	);
*)

	let file_info_handle = Unix.open_process_in (open_me) in (* dev_null is defined in common/types.ml *)
	let file_info = input_line file_info_handle in
	(match Unix.close_process_in file_info_handle with
		| Unix.WEXITED 1 -> (print "\nWARNING: avs2yuv exited with error 1; avs2yuv responded:\n  %s" file_info)
		| Unix.WEXITED x -> (if x <> 0 then (print "\nWARNING: avs2yuv exited with error %d\n" x))
		| Unix.WSIGNALED x -> print "WARNING: avs2yuv was killed with signal %d\n" x
		| Unix.WSTOPPED x -> print "WARNING: avs2yuv was stopped by signal %d\n" x
	);
(*	if debug_file then print "GOT NORMAL INFO %S\n" file_info;*)

(*
	if not (Str.string_match rx file_info 0) then (print "\nERROR: AVS file '%s' does not seem to be valid\n" o.o_input_avs; exit 1);

	let res_x = int_of_string (Str.matched_group 1 file_info) in
	let res_y = int_of_string (Str.matched_group 2 file_info) in
	let fps_n = int_of_string (Str.matched_group 3 file_info) in
	let fps_d = int_of_string (Str.matched_group 4 file_info) in
	let num_frames = int_of_string (Str.matched_group 5 file_info) in
	let fps_f = float_of_int fps_n /. float_of_int fps_d in
*)

	let (res_x, res_y, fps_n, fps_d, num_frames, fps_f) = (match match_this_string file_info with
		| Some x -> x
		| None -> (print "\nERROR: AVS file '%s' does not seem to be valid\n" o.o_input_avs; exit 1)
	) in

	(* 1st pass AVS (just to make sure it's right) *)
	let file_info_handle = Unix.open_process_in (Printf.sprintf "\"\"%s\" -frames 1 -raw -o %s \"%s\" 2>&1\"" avs2yuv dev_null o.o_input_first_avs) in (* dev_null is defined in common/types.ml *)
	let file_info_1 = input_line file_info_handle in
	(match Unix.close_process_in file_info_handle with
		| Unix.WEXITED 1 -> (print "\nWARNING: avs2yuv exited with error 1; first-pass AVS file \"%s\" may have an error\n" o.o_input_first_avs)
		| Unix.WEXITED x -> (if x <> 0 then (print "\nERROR: avs2yuv exited with error %d\n" x; exit x))
		| Unix.WSIGNALED x -> print "WARNING: avs2yuv was killed with signal %d\n" x
		| Unix.WSTOPPED x -> print "WARNING: avs2yuv was stopped by signal %d\n" x
	);
	if debug_file then print "GOT FIRST INFO  %S\n" file_info_1;

(*
	if not (Str.string_match rx file_info_1 0) then (print "\nERROR: AVS file '%s' does not seem to be valid\n" o.o_input_first_avs; exit 1);

	if (
		res_x <> int_of_string (Str.matched_group 1 file_info_1) ||
		res_y <> int_of_string (Str.matched_group 2 file_info_1) ||
		fps_n <> int_of_string (Str.matched_group 3 file_info_1) ||
		fps_d <> int_of_string (Str.matched_group 4 file_info_1) ||
		num_frames <> int_of_string (Str.matched_group 5 file_info_1)
	) then (
		(* Data don't match up! *)
		print "\nERROR: Info from first pass AVS does not match up with second pass AVS\n";
		print "1st: %s\n" file_info_1;
		print "2nd: %s\n" file_info;
		exit 1
	);
*)

	(match match_this_string file_info_1 with
		| Some (res_x', res_y', fps_n', fps_d', num_frames', fps_f') when res_x = res_x' && res_y = res_y' && fps_n = fps_n' && num_frames = num_frames' -> () (* OK *)
		| Some x -> (print "\nERROR: Info from first pass AVS does not match up with second pass AVS\n"; print "1st: %s\n" file_info_1; print "2nd: %s\n" file_info; exit 1)
		| None -> (print "\nERROR: AVS file '%s' does not seem to be valid\n" o.o_input_first_avs; exit 1)
	);


	(* Fast AVS (also to make sure the info matches up) *)
	let file_info_handle = Unix.open_process_in (Printf.sprintf "\"\"%s\" -frames 1 -raw -o %s \"%s\" 2>&1\"" avs2yuv dev_null o.o_input_fast_avs) in (* dev_null is defined in common/types.ml *)
	let file_info_fast = input_line file_info_handle in
	(match Unix.close_process_in file_info_handle with
		| Unix.WEXITED 1 -> (print "\nERROR: avs2yuv exited with error 1; fast AVS file \"%s\" probably has an error\n" o.o_input_fast_avs)
		| Unix.WEXITED x -> (if x <> 0 then (print "\nERROR: avs2yuv exited with error %d\n" x; exit x))
		| Unix.WSIGNALED x -> print "WARNING: avs2yuv was killed with signal %d\n" x
		| Unix.WSTOPPED x -> print "WARNING: avs2yuv was stopped by signal %d\n" x
	);
	if debug_file then print "GOT FAST INFO   %S\n" file_info_fast;

(*
	if not (Str.string_match rx file_info_fast 0) then (print "\nERROR: AVS file '%s' does not seem to be valid\n" o.o_input_fast_avs; exit 1);

	let fast_res_x = int_of_string (Str.matched_group 1 file_info_fast) in
	let fast_res_y = int_of_string (Str.matched_group 2 file_info_fast) in

	if (
		fps_n <> int_of_string (Str.matched_group 3 file_info_fast) ||
		fps_d <> int_of_string (Str.matched_group 4 file_info_fast) ||
		num_frames <> int_of_string (Str.matched_group 5 file_info_fast)
	) then (
		(* Data don't match up! *)
		print "\nERROR: Info from fast AVS does not match up with second pass AVS\n";
		print "fast: %s\n" file_info_fast;
		print "2nd:  %s\n" file_info;
		exit 1
	);
*)

	let (fast_res_x, fast_res_y) = (match match_this_string file_info_fast with
		| Some (res_x', res_y', fps_n', fps_d', num_frames', fps_f') when fps_n = fps_n' && num_frames = num_frames' -> (res_x', res_y')
		| Some x -> (print "\nERROR: Info from fast AVS does not match up with second pass AVS\n"; print "fast: %s\n" file_info_fast; print "2nd:  %s\n" file_info; exit 1)
		| None -> (print "\nERROR: AVS file '%s' does not seem to be valid\n" o.o_input_fast_avs; exit 1)
	) in

	let (get, put) = Unix.open_process (Printf.sprintf "\"\"%s\" -raw -slave -o - \"%s\" 2>%s\"" avs2yuv o.o_input_fast_avs dev_null) in

	if debug_file then (
		print "INFO:\n";
		print " i_res_x = %d\n" res_x;
		print " i_res_y = %d\n" res_y;
		print " i_fps_n = %d\n" fps_n;
		print " i_fps_d = %d\n" fps_d;
		print " i_fps_f = %f\n" fps_f;
		print " i_num_frames = %d\n" num_frames;
		print " i_bytes_y = %d\n" (res_x * res_y);
		print " i_bytes_uv = %d\n" ((res_x * res_y) lsr 2);
		print " i_bytes_per_frame = %d\n" (res_x * res_y * 3 / 2);
		print " multiplier for kbps to fbits = %f\n" (fps_f /. 1000.);
		print " fast_res_x = %d\n" fast_res_x;
		print " fast_res_y = %d\n" fast_res_y;
	);

	(* Get the frame offset and the number of frames to use *)
	let offset = min num_frames o.o_seek in
	let frames = (
		let max_frames = num_frames - offset in
		let real_specified_frames = (if o.o_frames = 0 then max_frames else o.o_frames) in
		min max_frames real_specified_frames
	) in

	let len_y = res_x * res_y in
	let len_uv = len_y lsr 2 in
	let len_frame = len_y + len_uv * 2 in

	let len_y_fast = fast_res_x * fast_res_y in
	let len_uv_fast = len_y_fast lsr 2 in
(*	let len_frame_fast = len_y_fast + len_uv_fast * 2 in*)

	let get_fast_frame = (
		let get_frame_mutex = Mutex.create () in
		fun x -> (
			Mutex.lock get_frame_mutex;
			output_string put (Printf.sprintf "%d\n" (x + offset));
			flush put;
			let guts_y = String.create len_y_fast in
			let guts_u = String.create len_uv_fast in
			let guts_v = String.create len_uv_fast in
			really_input get guts_y 0 len_y_fast;
			really_input get guts_u 0 len_uv_fast;
			really_input get guts_v 0 len_uv_fast;
			Mutex.unlock get_frame_mutex;
			(guts_y, guts_u, guts_v)
		)
	) in
	let get_fast_frame_y a = (
		let (y,_,_) = get_fast_frame a in
		y
	) in

	(* BIG comparing function *)
	let fast_every_nth_pixel = int_of_float (float_of_int fast_res_x *. 1.6180340) in (* Spiralicious! *)
	let fast_frame_diff a b = (
		let rec comp_this so_far on_now = if on_now >= len_y_fast then so_far else (
			comp_this (so_far + abs (Char.code a.[on_now] - Char.code b.[on_now])) (on_now + fast_every_nth_pixel)
		) in
		comp_this 0 0
	) in

(*
	print "IF THIS WAS RELEASED, YELL AT OMION!\n";
	exit 42;
*)

	{
		i_res_x = res_x;
		i_res_y = res_y;
		i_fps_n = fps_n;
		i_fps_d = fps_d;
		i_fps_f = fps_f;
		i_num_frames = frames;
		i_bytes_y = len_y;
		i_bytes_uv = len_uv;
		i_bytes_per_frame = len_frame;
		i_get_fast_frame = get_fast_frame;
		i_get_fast_frame_y = get_fast_frame_y;
		i_fast_res_x = fast_res_x;
		i_fast_res_y = fast_res_y;
		i_fast_frame_diff = fast_frame_diff;
		i_avs2yuv = avs2yuv;
(*
		i_frame_diff = frame_diff;
		i_get_frame_with_string = get_frame_with_string;
		i_get_frame = get_frame;
		i_frame_put = put;
		i_frame_get = get;
*)
	}
) in

(***************)
(* COMPRESSION *)
(***************)
(*
	FMA 44     25%
	Serenity   25%
	Neverjetni 33%
	FMA 45     26.3%
	Ame        34.6%
*)
	let supported_compressions = "\x00\x01" in
	let compression_setting = (
		let last_printed_ref = ref 0.0 in

		let compressed_01_ref = ref 0L in
		let uncompressed_01_ref = ref 0L in
		function
		| '\x00' -> (
			(* NO COMPRESSION *)
			print "using compression type 0 (no compression)\n";
			let thread_string = String.create (max_string_length + 8) in
			String.blit "FRAM" 0 thread_string 0 4;
			String.blit max_string_length_bin 0 thread_string 4 4;

			fun num_frames get sock -> (
				let max_64 = Int64.of_int max_string_length in
				let total_bytes_64 = Int64.mul (Int64.of_int num_frames) (Int64.of_int i.i_bytes_per_frame) in
				let rec send_bytes num_64 = (
					if num_64 <= max_64 then (
						let num = Int64.to_int num_64 in
						really_input get thread_string 8 num;
						Net.send_full sock ~len:num thread_string;
						String.blit max_string_length_bin 0 thread_string 4 4 (* Reset the length! Or else! *)
					) else (
						really_input get thread_string 8 max_string_length;
						Net.send_full sock thread_string;
						send_bytes (Int64.sub num_64 max_64)
					)
				) in
				send_bytes total_bytes_64;
			)
		)
		| '\x01' -> (
			(* PAETH / HUFFMAN *)
			print "using compression type 1 (Paeth / Huffman)\n";

			(* String to keep the raw frame Y data; also, output it if the compressed data is larger than it *)
			let temp_y = String.create (i.i_bytes_y + 9) in
			String.blit "ZFRM" 0 temp_y 0 4;
			String.blit (Pack.packN i.i_bytes_y) 0 temp_y 4 4;
			temp_y.[8] <- '\x00'; (* This means no compression *)
	
			(* Same for the UV part *)
			let bytes_uv = i.i_bytes_uv lsl 1 in
			let temp_u = String.create (bytes_uv + 9) in
			String.blit "ZFRM" 0 temp_u 0 4;
			String.blit (Pack.packN bytes_uv) 0 temp_u 4 4;
			temp_u.[8] <- '\x00';

			(* Compressed Y string *)
			let y_string = String.create (i.i_bytes_y + 9) in
			String.blit "ZFRM\x00\x00\x00\x00\x01" 0 y_string 0 9;
			let y_buff = Huff.huff_buff_of_string ~start:9 y_string in

			(* Compressed UV string *)
			let u_string = String.create (bytes_uv + 9) in
			String.blit "ZFRM\x00\x00\x00\x00\x02" 0 u_string 0 9;
			let u_buff = Huff.huff_buff_of_string ~start:9 u_string in

			let paeth a b c = (
				let p = a + b - c in
				let pa = abs (p - a) in
				let pb = abs (p - b) in
				let pc = abs (p - c) in
				if pa <= pb && pa <= pc then (
					a
				) else if pb <= pc then (
					b
				) else (
					c
				)
			) in

			fun num_frames get sock -> (
				for frame = 1 to num_frames do
					really_input get temp_y 9 i.i_bytes_y;
					really_input get temp_u 9 bytes_uv;

					Huff.set_huff_buff_pos y_buff 9;
					Huff.set_huff_buff_pos u_buff 9;

					let rec do_frame_y x y (out_val,out_bits) = (
						if y >= i.i_res_y then (
							(* Oops. Fell out of the bottom *)
							Huff.write_char y_buff out_val
						) else if x >= i.i_res_x then (
							(* Oops. Fell out of the side *)
							do_frame_y 0 (succ y) (out_val,out_bits)
						) else (
							(* Normal pixel *)
							let a = (if x = 0 then 0 else Char.code temp_y.[9 + y * i.i_res_x + (x - 1)]) in
							let b = (if y = 0 then 0 else Char.code temp_y.[9 + (y - 1) * i.i_res_x + x]) in
							let c = (if x = 0 || y = 0 then 0 else Char.code temp_y.[9 + (y - 1) * i.i_res_x + (x - 1)]) in
							let p = Char.code temp_y.[9 + y * i.i_res_x + x] in
							let encode_me = (p - paeth a b c) land 0xFF in
							let new_val_bits = Huff.add_char Huff.y_data y_buff (out_val,out_bits) encode_me in
							do_frame_y (succ x) y new_val_bits
						)
					) in

					let (write_me, write_bytes) = (try
						do_frame_y 0 0 (0,0);
						Huff.innards_of_huff_buff y_buff
					with
						Huff.Buffer_full -> (print "Y sending uncompressed frame\n"; (temp_y, (i.i_bytes_y + 9)))
					) in

(* TEMP MD5 *)
let md5 = Digest.substring temp_y 9 i.i_bytes_y in
(*	let md5 = "\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00" in*)
(*print "Y MD5 = %s\n" (to_hex md5);*)
(* ! TEMP MD5 *)

					Net.send_full sock ~len:(write_bytes - 8 + 16) (String.sub write_me 0 write_bytes ^ md5);
(*					Net.send_full sock ~len:(write_bytes - 8) write_bytes;*)

if false then (
compressed_01_ref := Int64.add !compressed_01_ref (Int64.of_int (write_bytes - 8));
);
					(* Now do the UV stuff *)
					let u_res_x = i.i_res_x lsr 1 in
					let rec do_frame_u x y (out_val,out_bits) = (
						if y >= i.i_res_y then (
							Huff.write_char u_buff out_val
						) else if x >= u_res_x then (
							do_frame_u 0 (succ y) (out_val,out_bits)
						) else (
							let a = (if x = 0 then 128 else Char.code temp_u.[9 + y * u_res_x + (x - 1)]) in
							let b = (if y = 0 then 128 else Char.code temp_u.[9 + (y - 1) * u_res_x + x]) in
							let c = (if x = 0 || y = 0 then 128 else Char.code temp_u.[9 + (y - 1) * u_res_x + (x - 1)]) in
							let p = Char.code temp_u.[9 + y * u_res_x + x] in
							let encode_me = (p - paeth a b c) land 0xFF in
							let new_val_bits = Huff.add_char Huff.u_data u_buff (out_val,out_bits) encode_me in
							do_frame_u (succ x) y new_val_bits
						)
					) in

					let (write_me, write_bytes) = (try
						do_frame_u 0 0 (0,0);
						Huff.innards_of_huff_buff u_buff
					with
						| Huff.Buffer_full -> (print "U sending uncompressed frame\n"; (temp_u, (bytes_uv + 9)))
						| Invalid_argument x -> (print "GOT invalid argument %s\n" x; raise (Invalid_argument x))
						| x -> (print "GOT %s\n" (Printexc.to_string x); raise x)
					) in

(* TEMP MD5 *)
let md5 = Digest.substring temp_u 9 bytes_uv in
(*let md5 = "\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00" in*)
(*print "U MD5 = %s\n" (to_hex md5);*)
(* ! TEMP MD5 *)

					Net.send_full sock ~len:(write_bytes - 8 + 16) (String.sub write_me 0 write_bytes ^ md5);

if false then (
compressed_01_ref := Int64.add !compressed_01_ref (Int64.of_int (write_bytes - 16));
uncompressed_01_ref := Int64.add !uncompressed_01_ref (Int64.of_int i.i_bytes_per_frame);
);
					(* Fake it, for now *)
(*				send sock "FRAM" (String.sub temp_u 9 bytes_uv);*)

if false then (
if !last_printed_ref +. 10.0 < Sys.time () then (
	last_printed_ref := Sys.time ();
	print "01: %.1f%% compression (%Ld / %Ld bytes)\n" (100.0 *. Int64.to_float !compressed_01_ref /. Int64.to_float !uncompressed_01_ref) !compressed_01_ref !uncompressed_01_ref;
);
);
				done
			)
		)
	) in




(**************************)
(* READY THE DIRECTORIES! *)
(**************************)

	(* AVS-specific temp dir *)
	(* The temp dir should be specific to the location and contents of the AVS file *)
	let avs_path_md5 = Digest.string o.o_input_avs_full in
	let avs_temp_dir = Filename.concat o.o_temp_dir (to_hex (string_xor o.o_input_avs_md5 avs_path_md5) ^ " " ^ (Filename.basename o.o_input_avs)) in
	print " AVS temp dir is %s\n" avs_temp_dir;
	(* Create the AVS temp dir if needed *)
	if is_file avs_temp_dir then (
		failwith (Printf.sprintf "AVS temp dir %S is a file!" avs_temp_dir)
	) else if not (is_dir avs_temp_dir) then (
		(* CREATE! *)
		Unix.mkdir avs_temp_dir 0o777;

		let f = open_out (Filename.concat avs_temp_dir "settings.txt") in
		Printf.fprintf f "Input file = %s\n" (if Filename.is_relative o.o_input_avs then Filename.concat Filename.current_dir_name o.o_input_avs else o.o_input_avs);
		close_out f;
	);

	(* Make a unique string describing the first pass options *)
	(* Including the file that was used as the first pass AVS *)
	let first_string = Printf.sprintf "%s\n%s\n%d+%d\n%d\n%s = %s\n" o.o_first (Marshal.to_string o.o_zones []) o.o_seek o.o_frames o.o_preseek o.o_input_first_avs_md5 o.o_input_first_avs_full in
	let first_md5 = to_hex (Digest.string first_string) in
	print " First string is %S, with MD5 %S\n" first_string first_md5;
	let first_dir = Filename.concat avs_temp_dir ("1-" ^ first_md5) in
	let first_stats = first_dir ^ ".txt" in

	(* Same with the second pass options *)
	let second_string = Printf.sprintf "%s\n%s\n%d+%d\n%d\n" o.o_second (Marshal.to_string o.o_zones []) o.o_seek o.o_frames o.o_preseek in
	let second_md5 = to_hex (Digest.string second_string) in
	print " Second string is %S, with MD5 %S\n" second_string second_md5;
	let second_dir = Filename.concat avs_temp_dir ("2-" ^ second_md5) in

	(* This indicates that everything has been done *)
	let done_file = Filename.concat avs_temp_dir ("DONE-" ^ first_md5 ^ second_md5 ^ ".txt") in

	if o.o_force || o.o_restart then (try Sys.remove done_file with _ -> ());

	if o.o_restart then (
		(* BALEETED! *)
		let dir_guts = Sys.readdir second_dir in
		Array.iter (fun x ->
			(try
				Unix.unlink (Filename.concat second_dir x);
			with
				_ -> print "WARNING: Failed to remove file \"%s\"\n" (Filename.concat second_dir x)
			)
		) dir_guts;

		let dir_guts = Sys.readdir first_dir in
		Array.iter (fun x ->
			(try
				Unix.unlink (Filename.concat first_dir x);
			with
				_ -> print "WARNING: Failed to remove file \"%s\"\n" (Filename.concat first_dir x)
			)
		) dir_guts;

		(try Unix.unlink first_stats with _ -> print "WARNING: Failed to remove file \"%s\"\n" first_stats)
	);


	if is_file done_file && not o.o_restart && not o.o_force then ( (* The o.o_restart and o.o_force are just in case done_file was not removed properly *)
		(try
			let handle = open_in done_file in
			let line = input_line handle in
			print "This job was already encoded to file \"%s\"\n" line;
			close_in handle;
		with
			_ -> print "This job was already done\n";
		)
	) else (
		(*
			dir OK, file OK -> do first again
			dir OK, file xx -> do first again
			dir xx, file OK -> first is done
			dir xx, file xx -> do first after making the dir
		*)

		let zone_string = List.fold_left (fun so_far -> function
			| {zone_start = za; zone_end = zb; zone_type = Zone_bitrate zf} -> (Printf.sprintf "%s%d,%d,b=%f" (if so_far = "" then "" else so_far ^ "/") za zb zf)
			| {zone_start = za; zone_end = zb; zone_type = Zone_q zq} -> (Printf.sprintf "%s%d,%d,q=%d" (if so_far = "" then "" else so_far ^ "/") za zb (int_of_float zq))
		) "" o.o_zones in


		if not (is_something first_dir) && is_file first_stats then (
			(* Done! *)
			print "not (is_something first_dir) && is_file first_stats\n";
		) else (
			print "not (not (is_something first_dir) && is_file first_stats)\n";
			if not (is_dir first_dir) then (
				(* Make the dir! *)
				print "not (is_dir first_dir)\n";
				(Unix.mkdir first_dir 0o777);

				(* Add some helpful files to it *)
				let f = open_out (Filename.concat first_dir "settings.txt") in
(*
				Printf.fprintf first_file "Input file = %s\n" (if Filename.is_relative o.o_input_avs      then Filename.concat current_dir_name o.o_input_avs      else o.o_input_avs);
				Printf.fprintf first_file "Fast file  = %s\n" (if Filename.is_relative o.o_input_fast_avs then Filename.concat current_dir_name o.o_input_fast_avs else o.o_input_fast_avs);
*)
				Printf.fprintf f "Options = %s\n" o.o_first;
				Printf.fprintf f "Zones   = %s\n" zone_string;
				Printf.fprintf f "Seek    = %d\n" o.o_seek;
				Printf.fprintf f "Frames  = %d\n" o.o_frames;
				Printf.fprintf f "Preseek = %d\n" o.o_preseek;
				close_out f;
			);

			(* Do first *)
			run_first_pass o i first_dir first_stats compression_setting supported_compressions;
			(* Delete dir *)

		);

		(* Let's see if this helps here *)
		Gc.full_major ();

		(* Now do the second! *)
		if not (is_dir second_dir) then (
			(* Make the dir! *)
			print "not (is_dir second_dir)\n";
			(Unix.mkdir second_dir 0o777);

			(* Add a handy file *)
			let f = open_out (Filename.concat second_dir "settings.txt") in
(*
			Printf.fprintf first_file "Input file = %s\n" (if Filename.is_relative o.o_input_avs      then Filename.concat current_dir_name o.o_input_avs      else o.o_input_avs);
			Printf.fprintf first_file "Fast file  = %s\n" (if Filename.is_relative o.o_input_fast_avs then Filename.concat current_dir_name o.o_input_fast_avs else o.o_input_fast_avs);
*)
			Printf.fprintf f "Options = %s\n" o.o_second;
			Printf.fprintf f "Zones   = %s\n" zone_string;
			Printf.fprintf f "Seek    = %d\n" o.o_seek;
			Printf.fprintf f "Frames  = %d\n" o.o_frames;
			Printf.fprintf f "Preseek = %d\n" o.o_preseek;
			close_out f;
		);

		(* Do second *)
		run_second_pass o i second_dir first_stats compression_setting supported_compressions;

		(* Make the done file *)
		let done_handle = open_out done_file in
		Printf.fprintf done_handle "%s\n" (if Filename.is_relative o.o_output_file then Filename.concat (Sys.getcwd ()) o.o_output_file else o.o_output_file);
		close_out done_handle;

		(* Now remove the stuff *)
		if true then (
			let dir_guts = Sys.readdir second_dir in
			Array.iter (fun x ->
				(try
					Unix.unlink (Filename.concat second_dir x);
				with
					_ -> print "WARNING: Failed to remove file \"%s\"\n" (Filename.concat second_dir x)
				)
			) dir_guts;
			(* Now try to remove the whole dir *)
			(try
				Unix.rmdir second_dir
			with
				_ -> print "WARNING: Failed to remove second dir \"%s\"\n" second_dir
			)
		);

	)

);; (* THE END! *)

print "\nTIME %f\n" (Sys.time () -. time);;
