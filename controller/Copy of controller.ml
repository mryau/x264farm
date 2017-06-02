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
				second_pass_bitrate_ref := Percent (int_of_string (String.sub x 0 (String.length x - 1)))
			with
			| Failure "int_of_string" -> raise (Unsupported_argument "Bitrate needs to be in the form \"123%\" or \"123.4kbps\"")
		) else if (String.length x > 4 && String.lowercase (String.sub x (String.length x - 4) 4) = "kbps") then (
			try
				second_pass_bitrate_ref := Kbps (float_of_string (String.sub x 0 (String.length x - 4)))
			with
			| Failure "float_of_string" -> raise (Unsupported_argument "Bitrate needs to be in the form \"123%\" or \"123.4kbps\"")
		) else (
			second_pass_bitrate_ref := Percent (int_of_string x)
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
		zones_ref := parse_zones_rec str
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

	let opt_help     = StdOpt.store_true                         () in
	let opt_bitrate  = StdOpt.str_option   ~default:"100%"       ~metavar:"" () in
	let opt_first    = StdOpt.str_option                         ~metavar:"" () in
	let opt_second   = StdOpt.str_option                         ~metavar:"" () in
	let opt_avs      = StdOpt.str_option                         ~metavar:"" () in
	let opt_firstavs = StdOpt.str_option                         ~metavar:"" () in
	let opt_fastavs  = StdOpt.str_option                         ~metavar:"" () in
	let opt_output   = StdOpt.str_option   ~default:"output.mkv" ~metavar:"" () in

	let opt_preseek  = StdOpt.int_option   ~default:0            ~metavar:"" () in
	let opt_force    = StdOpt.store_true                         () in
	let opt_restart  = StdOpt.store_true                         () in
	let opt_config   = StdOpt.str_option   ~default:"config.xml" ~metavar:"" () in

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
	let opt_bpratio  = StdOpt.float_option ~default:1.3          ~metavar:"" () in

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
	OptParser.add p ~group:group_control                 ~long_name:"preseek" ~help:(Printf.sprintf "Number of frames to render before each job start (%d)" (Opt.get opt_preseek)) opt_preseek;
	OptParser.add p ~group:group_control                 ~long_name:"force"   ~help:"Resume encode even if it was already finished" opt_force;
	OptParser.add p ~group:group_control                 ~long_name:"restart" ~help:"Remove all temp files and start over" opt_restart;
	OptParser.add p ~group:group_control                 ~long_name:"config"  ~help:(Printf.sprintf "File for agent configuration settings (%S)" (Opt.get opt_config)) opt_config;

	let group_1st = OptParser.add_group p "First pass split parameters" in
	OptParser.add p ~group:group_1st                     ~long_name:"batch"  ~help:(Printf.sprintf "How many frames to give out at a time (%d)" (Opt.get opt_batch)) opt_batch;
	OptParser.add p ~group:group_1st                     ~long_name:"split"  ~help:(Printf.sprintf "How many frames to check for optimal frame length (%d)" (Opt.get opt_split)) opt_split;
	OptParser.add p ~group:group_1st                     ~long_name:"thresh" ~help:(Printf.sprintf "Threshold for early-abort of the frame change (%F)" (Opt.get opt_thresh)) opt_thresh;

	let group_2nd = OptParser.add_group p "Selective third-pass parameters" in
	OptParser.add p ~group:group_2nd                     ~long_name:"3thresh" ~help:(Printf.sprintf "Max accuracy to consider for 3rd pass (0..1) (%F)" (Opt.get opt_3thresh)) opt_3thresh;
	OptParser.add p ~group:group_2nd                     ~long_name:"3gops"   ~help:(Printf.sprintf "Max number of GOPs to consider for 3rd pass (%d)" (Opt.get opt_3gops)) opt_3gops;
	OptParser.add p ~group:group_2nd                     ~long_name:"3ratio"  ~help:(Printf.sprintf "Max percentage of GOPs to consider for 3rd pass (%F)" (Opt.get opt_3ratio)) opt_3ratio;
	OptParser.add p ~group:group_2nd                     ~long_name:"rerc"    ~help:(Printf.sprintf "Number of GOPs betwenn RC passes (%d)" (Opt.get opt_rerc)) opt_rerc;

	let group_x264 = OptParser.add_group p "These are the same as x264 and should not be used in the -1 and -2 strings" in
	OptParser.add p ~group:group_x264                    ~long_name:"zones"    ~help:(Printf.sprintf "(%s)" (Opt.get opt_zones)) opt_zones;
	OptParser.add p ~group:group_x264                    ~long_name:"seek"     ~help:(Printf.sprintf "(%d)" (Opt.get opt_seek)) opt_seek;
	OptParser.add p ~group:group_x264                    ~long_name:"frames"   ~help:(Printf.sprintf "(%d)" (Opt.get opt_frames)) opt_frames;
	OptParser.add p ~group:group_x264                    ~long_name:"qcomp"    ~help:(Printf.sprintf "(%F)" (Opt.get opt_qcomp)) opt_qcomp;
	OptParser.add p ~group:group_x264                    ~long_name:"cplxblur" ~help:(Printf.sprintf "(%F)" (Opt.get opt_cplxblur)) opt_cplxblur;
	OptParser.add p ~group:group_x264                    ~long_name:"qblur"    ~help:(Printf.sprintf "(%F)" (Opt.get opt_qblur)) opt_qblur;
	OptParser.add p ~group:group_x264                    ~long_name:"qpmin"    ~help:(Printf.sprintf "(%d)" (Opt.get opt_qpmin)) opt_qpmin;
	OptParser.add p ~group:group_x264                    ~long_name:"qpmax"    ~help:(Printf.sprintf "(%d)" (Opt.get opt_qpmax)) opt_qpmax;
	OptParser.add p ~group:group_x264                    ~long_name:"qpstep"   ~help:(Printf.sprintf "(%d)" (Opt.get opt_qpstep)) opt_qpstep;
	OptParser.add p ~group:group_x264                    ~long_name:"ipratio"  ~help:(Printf.sprintf "(%F)" (Opt.get opt_ipratio)) opt_ipratio;
	OptParser.add p ~group:group_x264                    ~long_name:"bpratio"  ~help:(Printf.sprintf "(%F)" (Opt.get opt_bpratio)) opt_bpratio;

	let others = OptParser.parse_argv p in
	List.iter (fun x -> print "%s\n" x) others;
	exit 763;
(*
	let parse_help = (
		let p = (fun x -> Printf.kprintf (fun y -> y) x) in
		let a = [|
			(* 12345678901234567890123456789012345678901234567890123456789012345678901234567890 *)
			p "";
			p "General x264 parameters:";
			p "  -h --help           Print this info";
			p "  -B --bitrate <x>    Second pass bitrate (100%%)";
			p "                        <x> may be in %% or kbps, as in \"-B 400kbps\"";
			p "  -1 --first <str>    First pass setting string (\"\")";
			p "  -2 --second <str>   Second pass setting string (\"\")";
			p "  -i --avs <str>      The input AVS file (\"\")";
			p "     --firstavs <str> The AVS file to use for the first pass (--avs)";
			p "     --fastavs <str>  Input AVS for first-pass split testing (--firstavs)";
			p "  -o --output <str>   Output MKV file (%S)" !output_file_ref;
			p "";
			p "x264farm control parameters:";
			p "     --preseek <int>  Number of frames to render before each job start (%d)" !preseek_ref;
			p "     --force          Resume encode even if it was already finished";
			p "     --restart        Remove all temp files and start over";
			p "     --config <str>   File for agent configuration settings (%S)" !config_file_ref;
			p "";
			p "First pass split parameters:";
			p "     --batch <int>    How many frames to give out at a time (%d)" !optimal_batch_length_ref;
			p "     --split <int>    How many frames to check for optimal frame length (%d)" !split_frame_length_ref;
			p "     --thresh <float> Threshold for early-abort of the frame change (%F)" !split_thresh_ref;
			p "";
			p "Selective third-pass parameters:";
			p "     --3thresh <float> Max accuracy to consider for 3rd pass (0..1) (%F)" !third_pass_threshold_ref;
			p "     --3gops <int>     Max number of GOPs to consider for 3rd pass (%d)" !third_pass_max_gops_ref;
			p "     --3ratio <float>  Max percentage of GOPs to consider for 3rd pass (%F)" !third_pass_max_gop_ratio_ref;
			p "     --rerc <int>      Number of GOPs betwenn RC passes (%d)" !rerc_gops_ref;
			p "";
			p "These are the same as x264 and should be used outside the -1 and -2 strings:";
			p "     --zones <zone0>/<zone1>/...  (\"\")";
			p "     --seek <int>       (%d)" !seek_ref;
			p "     --frames <int>     (%d)" !frames_ref;
			p "     --qcomp <float>    (%F)" !rc_qcomp_ref;
			p "     --cplxblur <float> (%F)" !rc_cplxblur_ref;
			p "     --qblur <float>    (%F)" !rc_qblur_ref;
			p "     --qpmin <int>      (%d)" !rc_qpmin_ref;
			p "     --qpmax <int>      (%d)" !rc_qpmax_ref;
			p "     --qpstep <int>     (%d)" !rc_qpstep_ref;
			p "     --ipratio <float>  (%F)" !rc_ipratio_ref;
			p "     --bpratio <float>  (%F)" !rc_bpratio_ref;
		|] in
		Array.fold_left (fun so_far gnu -> so_far ^ gnu ^ "\n") "" a
	) in
	let parse_this = (
		let nos = Getopt.noshort in
(*		let nol = Getopt.nolong in*)
		let set_string a = Some (fun x -> a := x) in (* Sets the reference given to the argument *)
		let set_int    a = Some (fun x -> a := int_of_string x) in
		let set_float  a = Some (fun x -> a := float_of_string x) in
		let set_true   a = Some (fun () -> a := true) in
(*		let set_false  a = Some (fun () -> a := false) in*)
		[
			('h',"help",    Some (fun () -> print "%s" parse_help; exit 1),None);
			('B',"bitrate", None,Some parse_bitrate);
			('1',"first",   None,set_string first_option_ref);
			('2',"second",  None,set_string second_option_ref);
			('i',"avs",     None,set_string input_avs_ref);
			(nos,"firstavs",None,set_string input_first_avs_ref);
			(nos,"fastavs", None,set_string input_fast_avs_ref);
			('o',"output",  None,set_string output_file_ref);

			(nos,"preseek", None,set_int preseek_ref);
			(nos,"force",   set_true force_ref,None);
			(nos,"restart", set_true restart_ref,None);
			(nos,"config",  None,set_string config_file_ref);

			(nos,"batch",   None,set_int optimal_batch_length_ref);
			(nos,"split",   None,set_int split_frame_length_ref);
			(nos,"thresh",  None,set_float split_thresh_ref);

			(nos,"3thresh", None,set_float third_pass_threshold_ref);
			(nos,"3gops",   None,set_int third_pass_max_gops_ref);
			(nos,"3ratio",  None,set_float third_pass_max_gop_ratio_ref);
			(nos,"rerc",    None,set_int rerc_gops_ref);

			(nos,"zones",   None,Some parse_zones);
			(nos,"seek",    None,set_int seek_ref);
			(nos,"frames",  None,set_int frames_ref);
			(nos,"qcomp",   None,set_float rc_qcomp_ref);
			(nos,"cplxblur",None,set_float rc_cplxblur_ref);
			(nos,"qblur",   None,set_float rc_qblur_ref);
			(nos,"qpmin",   None,set_int rc_qpmin_ref);
			(nos,"qpmax",   None,set_int rc_qpmax_ref);
			(nos,"qpstep",  None,set_int rc_qpstep_ref);
			(nos,"ipratio", None,set_float rc_ipratio_ref);
			(nos,"bpratio", None,set_float rc_bpratio_ref);
		]
	) in
	(try
		Getopt.parse_cmdline parse_this (fun x -> input_option_file_ref := x)
	with
		Getopt.Error x -> (print "\nERROR: %s\n%s" x parse_help; exit 1)
	);
*)
	let parser = 7 in

(*	Printf.printf "%s" parse_help;*)
(*	exit 2348;*)

	(* Read the file specified on the command line (if any) *)
	let valid_input_option_file = if Sys.file_exists !input_option_file_ref then (
		if debug_file then Printf.printf "Read stuff from %S\n" !input_option_file_ref;
		try (
			let option_handle = open_in !input_option_file_ref in
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
	if not valid_input_option_file then (
		print "WARNING: Input option file %s not found, or an error occured parsing the file\n" !input_option_file_ref
	);

	print "DED\n";
	exit 3;

	if debug_file then (
		print "OPTIONS:\n";
		print " -b        %s\n" (match !second_pass_bitrate_ref with | Percent x -> (string_of_int x ^ "%") | Kbps x -> (string_of_float x ^ "kbps"));
		print " --first   %S\n" !first_option_ref;
		print " --second  %S\n" !second_option_ref;
		print " --avs     %S\n" !input_avs_ref;
		print " --fastavs %S\n" !input_fast_avs_ref;
		print " --zones  ";
		List.iter (function | {zone_start=s; zone_end=e; zone_type=Zone_bitrate b} -> print " %d,%d,b=%f" s e b | {zone_start=s; zone_end=e; zone_type=Zone_q q} -> print " %d,%d,q=%f" s e q) !zones_ref;
		print "\n";
		print " --seek    %d\n" !seek_ref;
		print " --frames  %d\n" !frames_ref;
		print " --batch   %d\n" !optimal_batch_length_ref;
		print " --split   %d\n" !split_frame_length_ref;
		print " --thresh  %f\n" !split_thresh_ref;
		print " --preseek %d\n" !preseek_ref;
		print " --force   %B\n" !force_ref;
		print " --restart %B\n" !restart_ref;
		print " --config  %S\n" !config_file_ref;
		print " --3thresh %f\n" !third_pass_threshold_ref;
		print " --3gops   %d\n" !third_pass_max_gops_ref;
		print " --3ratio  %f\n" !third_pass_max_gop_ratio_ref;
		print " -o        %S\n" !output_file_ref;
	);

	let input_avs = if is_file !input_avs_ref then !input_avs_ref else (print "ERROR: input AVS \"%s\" is not a file" !input_avs_ref; exit 1) in
	let input_first_avs = if is_file !input_first_avs_ref then !input_first_avs_ref else input_avs in
	let input_fast_avs = if is_file !input_fast_avs_ref then !input_fast_avs_ref else input_first_avs in

	let first_option = !first_option_ref in
	if first_option = "" then Printf.eprintf "WARNING: First pass options are not set; this will use the default x264 settings\n";
	let second_option = !second_option_ref in
	if second_option = "" then Printf.eprintf "WARNING: Second pass options are not set; this will use the default x264 settings\n";
(*
	let config_file = try (
		match (Unix.LargeFile.stat !config_file_ref).Unix.LargeFile.st_kind with
		| Unix.S_REG -> !config_file_ref
		| _ -> (Printf.eprintf "WARNING: config file '%s' is not a file; using config.xml instead\n" !config_file_ref; "config.xml")
	) with
		Unix.Unix_error(Unix.ENOENT, "stat", _) -> (Printf.eprintf "WARNING: config file '%s' does not exist; using config.xml instead\n" !config_file_ref; "config.xml")
	in
*)

	let config_file = match (search_for_file !config_file_ref, search_for_file "config.xml") with
		| (Some x, _) -> x
		| (None, Some y) -> (print "WARNING: Config file \"%s\" does not exist; using \"%s\" instead\n" !config_file_ref y; y)
		| (None, None) -> (print "ERROR: No configuration file found; exiting\n"; exit 2)
	in
	print "Using config file \"%s\"\n" config_file;

	let output_file = try (
		match (Unix.LargeFile.stat !output_file_ref).Unix.LargeFile.st_kind with
		| Unix.S_REG -> (Printf.eprintf "WARNING: Output file '%s' already exists and will be overwritten!\n" !output_file_ref; flush stderr; !output_file_ref)
		| _ -> (Printf.eprintf "ERROR: Output file '%s' is not a file\n" !output_file_ref; exit 1)
	) with
		Unix.Unix_error(Unix.ENOENT, "stat", _) -> !output_file_ref
	in

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
		| _ -> (print "ERROR: Temp directory '%s' not a directory\n" !temp_dir_ref; exit 1)
	) with
		Unix.Unix_error(Unix.ENOENT, "stat", _) -> (print "ERROR: Temp directory '%s' does not exist\n" !temp_dir_ref; exit 1)
	in

	if debug_file then (
		print "CONFIG:\n";
		print " temp_dir: %S\n" temp_dir_base;
		print " agent_list 1:\n";
		List.iter (fun (name, (ip, pf, pt), n) -> print "  \"%s %d\" = %s %d-%d\n" name n ip pf pt) agents_1;
		print " agent_list 2:\n";
		List.iter (fun (name, (ip, pf, pt), n) -> print "  \"%s %d\" = %s %d-%d\n" name n ip pf pt) agents_2;
	);

	{
		o_first = first_option;
		o_second = second_option;
		o_zones = !zones_ref;
		o_seek = !seek_ref;
		o_frames = !frames_ref;
		o_batch_length = !optimal_batch_length_ref;
		o_split_frame_length = !split_frame_length_ref;
		o_split_thresh = !split_thresh_ref;
		o_output_file = output_file;
		o_input_avs = input_avs;
		o_input_fast_avs = input_fast_avs;
		o_input_first_avs = input_first_avs;
		o_second_pass_bitrate = !second_pass_bitrate_ref;
		o_third_pass_threshold = !third_pass_threshold_ref;
		o_third_pass_max_gops = !third_pass_max_gops_ref;
		o_third_pass_max_gop_ratio = !third_pass_max_gop_ratio_ref;
		o_ratecontrol_gops = !rerc_gops_ref;
		o_temp_dir = temp_dir_base;
		o_preseek = !preseek_ref;
		o_force = !force_ref;
		o_restart = !restart_ref;
		o_agents_1 = Array.of_list agents_1;
		o_agents_2 = Array.of_list agents_2;

		o_rc_qcomp = !rc_qcomp_ref;
		o_rc_cplxblur = !rc_cplxblur_ref;
		o_rc_qblur = !rc_qblur_ref;
		o_rc_qpmin = !rc_qpmin_ref;
		o_rc_qpmax = !rc_qpmax_ref;
		o_rc_qpstep = !rc_qpstep_ref;
		o_rc_ipratio = !rc_ipratio_ref;
		o_rc_pbratio = !rc_bpratio_ref;
	}
) in

(*********************************)
(* FIND OUT STUFF ABOUT THE FILE *)
(*********************************)
let i = (

	(* 2nd pass AVS *)
	let file_info_handle = Unix.open_process_in (Printf.sprintf "avs2yuv -frames 1 -raw -o %s \"%s\" 2>&1" dev_null o.o_input_avs) in (* dev_null is defined in common/types.ml *)
	let file_info = input_line file_info_handle in
	(match Unix.close_process_in file_info_handle with
		| Unix.WEXITED 1 -> (print "ERROR: avs2yuv exited with error 1; AVS file \"%s\" probably has an error\n" o.o_input_avs; exit 1)
		| Unix.WEXITED x -> (if x <> 0 then (print "ERROR: avs2yuv exited with error %d\n" x; exit x))
		| Unix.WSIGNALED x -> print "WARNING: avs2yuv was killed with signal %d\n" x
		| Unix.WSTOPPED x -> print "WARNING: avs2yuv was stopped by signal %d\n" x
	);
	if debug_file then print "GOT NORMAL INFO %S\n" file_info;

	let rx = Str.regexp ".*: \\([0-9]+\\)x\\([0-9]+\\), \\([0-9]+\\)/\\([0-9]+\\) fps, \\([0-9]+\\) frames" in
	if not (Str.string_match rx file_info 0) then (print "ERROR: AVS file '%s' does not seem to be valid\n" o.o_input_avs; exit 1);

	let res_x = int_of_string (Str.matched_group 1 file_info) in
	let res_y = int_of_string (Str.matched_group 2 file_info) in
	let fps_n = int_of_string (Str.matched_group 3 file_info) in
	let fps_d = int_of_string (Str.matched_group 4 file_info) in
	let num_frames = int_of_string (Str.matched_group 5 file_info) in
	let fps_f = float_of_int fps_n /. float_of_int fps_d in

	(* 1st pass AVS (just to make sure it's right) *)
	let file_info_handle = Unix.open_process_in (Printf.sprintf "avs2yuv -frames 1 -raw -o %s \"%s\" 2>&1" dev_null o.o_input_first_avs) in (* dev_null is defined in common/types.ml *)
	let file_info_1 = input_line file_info_handle in
	(match Unix.close_process_in file_info_handle with
		| Unix.WEXITED 1 -> (print "ERROR: avs2yuv exited with error 1; first-pas AVS file \"%s\" probably has an error\n" o.o_input_first_avs)
		| Unix.WEXITED x -> (if x <> 0 then (print "ERROR: avs2yuv exited with error %d\n" x; exit x))
		| Unix.WSIGNALED x -> print "WARNING: avs2yuv was killed with signal %d\n" x
		| Unix.WSTOPPED x -> print "WARNING: avs2yuv was stopped by signal %d\n" x
	);
	if debug_file then print "GOT FIRST INFO  %S\n" file_info_1;

	if not (Str.string_match rx file_info_1 0) then (print "ERROR: AVS file '%s' does not seem to be valid\n" o.o_input_first_avs; exit 1);

	if (
		res_x <> int_of_string (Str.matched_group 1 file_info_1) ||
		res_y <> int_of_string (Str.matched_group 2 file_info_1) ||
		fps_n <> int_of_string (Str.matched_group 3 file_info_1) ||
		fps_d <> int_of_string (Str.matched_group 4 file_info_1) ||
		num_frames <> int_of_string (Str.matched_group 5 file_info_1)
	) then (
		(* Data don't match up! *)
		print "Info from first pass AVS does not match up with second pass AVS\n";
		print "1st: %s\n" file_info_1;
		print "2nd: %s\n" file_info;
		exit 1
	);

	(* Fast AVS (also to make sure the info matches up) *)
	let file_info_handle = Unix.open_process_in (Printf.sprintf "avs2yuv -frames 1 -raw -o %s \"%s\" 2>&1" dev_null o.o_input_fast_avs) in (* dev_null is defined in common/types.ml *)
	let file_info_fast = input_line file_info_handle in
	(match Unix.close_process_in file_info_handle with
		| Unix.WEXITED 1 -> (print "ERROR: avs2yuv exited with error 1; first-pas AVS file \"%s\" probably has an error\n" o.o_input_fast_avs)
		| Unix.WEXITED x -> (if x <> 0 then (print "ERROR: avs2yuv exited with error %d\n" x; exit x))
		| Unix.WSIGNALED x -> print "WARNING: avs2yuv was killed with signal %d\n" x
		| Unix.WSTOPPED x -> print "WARNING: avs2yuv was stopped by signal %d\n" x
	);
	if debug_file then print "GOT FAST INFO   %S\n" file_info_fast;

	if not (Str.string_match rx file_info_fast 0) then (print "ERROR: AVS file '%s' does not seem to be valid\n" o.o_input_fast_avs; exit 1);

	let fast_res_x = int_of_string (Str.matched_group 1 file_info_fast) in
	let fast_res_y = int_of_string (Str.matched_group 2 file_info_fast) in

	if (
		fps_n <> int_of_string (Str.matched_group 3 file_info_fast) ||
		fps_d <> int_of_string (Str.matched_group 4 file_info_fast) ||
		num_frames <> int_of_string (Str.matched_group 5 file_info_fast)
	) then (
		(* Data don't match up! *)
		print "Info from fast AVS does not match up with second pass AVS\n";
		print "fast: %s\n" file_info_fast;
		print "2nd:  %s\n" file_info;
		exit 1
	);


	let (get, put) = Unix.open_process (Printf.sprintf "avs2yuv -raw -slave -o - \"%s\" 2>%s" o.o_input_fast_avs dev_null) in

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

(*
	let get_frame = (
		(* This is the full-quality version *)
		let (get, put) = Unix.open_process (Printf.sprintf "avs2yuv -raw -slave -o - \"%s\" 2>%s" o.o_input_avs dev_null) in
		let get_frame_mutex = Mutex.create () in
		fun x -> (
			Mutex.lock get_frame_mutex;
			output_string put (Printf.sprintf "%d\n" (x + offset));
			flush put;
			let guts = String.create len_frame in
			really_input get guts 0 len_frame;
			Mutex.unlock get_frame_mutex;
			guts
		)
	) in
*)
(*
	let get_frame_with_string = (
		let (get, put) = Unix.open_process (Printf.sprintf "avs2yuv -raw -slave -o - \"%s\" 2>%s" o.o_input_avs dev_null) in
		let get_frame_mutex = Mutex.create () in
		fun guts x -> (
			Mutex.lock get_frame_mutex;
			output_string put (Printf.sprintf "%d\n" (x + offset));
			flush put;
			really_input get guts 0 len_frame;
			Mutex.unlock get_frame_mutex
		)
	) in

	let get_frame x = (
		let guts = String.create len_frame in
		get_frame_with_string guts x;
		guts
	) in
*)
(*
Old one, before I split it into two pieces
	let get_frame = (
		let (get, put) = Unix.open_process (Printf.sprintf "avs2yuv -raw -slave -o - \"%s\" 2>%s" o.o_input_avs dev_null) in
		let get_frame_mutex = Mutex.create () in
		fun x -> (
			Mutex.lock get_frame_mutex;
			output_string put (Printf.sprintf "%d\n" x);
			flush put;
			let guts = String.create len_frame in
			really_input get guts 0 len_frame;
			Mutex.unlock get_frame_mutex;
			guts
		)
	) in
*)
	(* BIG comparing function *)
(*
	let every_nth_pixel = int_of_float (float_of_int res_x *. 1.6180340) in (* Spiralicious! *)
	let frame_diff a b = (
		let rec comp_this so_far on_now = if on_now >= len_y then so_far else (
			comp_this (so_far + abs (Char.code a.[on_now] - Char.code b.[on_now])) (on_now + every_nth_pixel)
		) in
		comp_this 0 0
	) in
*)
	let fast_every_nth_pixel = int_of_float (float_of_int fast_res_x *. 1.6180340) in (* Spiralicious! *)
	let fast_frame_diff a b = (
		let rec comp_this so_far on_now = if on_now >= len_y_fast then so_far else (
			comp_this (so_far + abs (Char.code a.[on_now] - Char.code b.[on_now])) (on_now + fast_every_nth_pixel)
		) in
		comp_this 0 0
	) in

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
(*
		i_frame_diff = frame_diff;
		i_get_frame_with_string = get_frame_with_string;
		i_get_frame = get_frame;
		i_frame_put = put;
		i_frame_get = get;
*)
	}
) in




(**************************)
(* READY THE DIRECTORIES! *)
(**************************)
	(* AVS-specific temp dir *)
	let avs_md5 = to_hex (Digest.file o.o_input_avs) in
	let avs_temp_dir = Filename.concat o.o_temp_dir (avs_md5 ^ " " ^ (Filename.basename o.o_input_avs)) in
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
	let first_string = Printf.sprintf "%s\n%s\n%d+%d\n%d\n" o.o_first (Marshal.to_string o.o_zones []) o.o_seek o.o_frames o.o_preseek in
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
		) else (
			if not (is_dir first_dir) then (
				(* Make the dir! *)
				(Unix.mkdir first_dir 0o777);

				(* Add some helpful files to it *)
print "a";
				let f = open_out (Filename.concat first_dir "settings.txt") in
print "b";
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
			run_first_pass o i first_dir first_stats;
			(* Delete dir *)

		);

		(* Let's see if this helps here *)
		Gc.full_major ();

		(* Now do the second! *)
		if not (is_dir second_dir) then (
			(* Make the dir! *)
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
		run_second_pass o i second_dir first_stats;

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
