type console_handle

type console_screen_buffer_info = {
	dwSizeX : int;
	dwSizeY : int;
	dwCursorX : int;
	dwCursorY : int;
	wAttributes : int;
	windowLeft : int;
	windowTop : int;
	windowRight : int;
	windowBottom : int;
	maxWindowSizeX : int;
	maxWindowSizeY : int
}

exception Console_error of string * int

external caml_std_input_handle  : unit -> int = "caml_STD_INPUT_HANDLE"
external caml_std_output_handle : unit -> int = "caml_STD_OUTPUT_HANDLE"
external caml_std_error_handle  : unit -> int = "caml_STD_ERROR_HANDLE"

(*external caml_fg_black        : unit -> int = "caml_FG_BLACK"*)
external caml_fg_lightblack   : unit -> int = "caml_FG_LIGHTBLACK"
external caml_fg_red          : unit -> int = "caml_FG_RED"
(*external caml_fg_lightred     : unit -> int = "caml_FG_LIGHTRED"*)
external caml_fg_green        : unit -> int = "caml_FG_GREEN"
(*external caml_fg_lightgreen   : unit -> int = "caml_FG_LIGHTGREEN"*)
external caml_fg_blue         : unit -> int = "caml_FG_BLUE"
(*
external caml_fg_lightblue    : unit -> int = "caml_FG_LIGHTBLUE"
external caml_fg_magenta      : unit -> int = "caml_FG_MAGENTA"
external caml_fg_lightmagenta : unit -> int = "caml_FG_LIGHTMAGENTA"
external caml_fg_cyan         : unit -> int = "caml_FG_CYAN"
external caml_fg_lightcyan    : unit -> int = "caml_FG_LIGHTCYAN"
external caml_fg_brown        : unit -> int = "caml_FG_BROWN"
external caml_fg_yellow       : unit -> int = "caml_FG_YELLOW"
external caml_fg_gray         : unit -> int = "caml_FG_GRAY"
external caml_fg_white        : unit -> int = "caml_FG_WHITE"
*)

(*external caml_bg_black        : unit -> int = "caml_BG_BLACK"*)
external caml_bg_lightblack   : unit -> int = "caml_BG_LIGHTBLACK"
external caml_bg_red          : unit -> int = "caml_BG_RED"
(*external caml_bg_lightred     : unit -> int = "caml_BG_LIGHTRED"*)
external caml_bg_green        : unit -> int = "caml_BG_GREEN"
(*external caml_bg_lightgreen   : unit -> int = "caml_BG_LIGHTGREEN"*)
external caml_bg_blue         : unit -> int = "caml_BG_BLUE"
(*
external caml_bg_lightblue    : unit -> int = "caml_BG_LIGHTBLUE"
external caml_bg_magenta      : unit -> int = "caml_BG_MAGENTA"
external caml_bg_lightmagenta : unit -> int = "caml_BG_LIGHTMAGENTA"
external caml_bg_cyan         : unit -> int = "caml_BG_CYAN"
external caml_bg_lightcyan    : unit -> int = "caml_BG_LIGHTCYAN"
external caml_bg_brown        : unit -> int = "caml_BG_BROWN"
external caml_bg_yellow       : unit -> int = "caml_BG_YELLOW"
external caml_bg_gray         : unit -> int = "caml_BG_GRAY"
external caml_bg_white        : unit -> int = "caml_BG_WHITE"
*)

external caml_get_std_handle : int -> console_handle = "caml_GetStdHandle"
external caml_set_console_text_attribute : console_handle -> int -> bool = "caml_SetConsoleTextAttribute"
external caml_get_console_screen_buffer_info : console_handle -> bool * console_screen_buffer_info = "caml_GetConsoleScreenBufferInfo"
external caml_get_console_text_attribute : console_handle -> bool * int = "caml_GetConsoleTextAttribute"
external caml_set_console_cursor_position : console_handle -> (int * int) -> bool = "caml_SetConsoleCursorPosition"

external caml_write : console_handle -> string -> bool * int = "caml_WriteConsole"

external caml_get_last_error : unit -> int = "caml_GetLastError"



(* CAML PART HERE *)
let std_input_handle  = caml_get_std_handle (caml_std_input_handle  ())
let std_output_handle = caml_get_std_handle (caml_std_output_handle ())
let std_error_handle  = caml_get_std_handle (caml_std_error_handle  ())

(** Foreground colors *)
let fg_black          = 0
let fg_lightblack     = caml_fg_lightblack     ()
let fg_red            = caml_fg_red            ()
let fg_lightred       = fg_red lor fg_lightblack
let fg_green          = caml_fg_green          ()
let fg_lightgreen     = fg_green lor fg_lightblack
let fg_blue           = caml_fg_blue           ()
let fg_lightblue      = fg_blue lor fg_lightblack
let fg_magenta        = fg_red lor fg_blue
let fg_lightmagenta   = fg_red lor fg_blue lor fg_lightblack
let fg_cyan           = fg_green lor fg_blue
let fg_lightcyan      = fg_green lor fg_blue lor fg_lightblack
let fg_brown          = fg_red lor fg_green
let fg_yellow         = fg_red lor fg_green lor fg_lightblack
let fg_gray           = fg_red lor fg_green lor fg_blue
let fg_white          = fg_red lor fg_green lor fg_blue lor fg_lightblack

(** Background colors *)
let bg_black          = 0
let bg_lightblack     = caml_bg_lightblack     ()
let bg_red            = caml_bg_red            ()
let bg_lightred       = bg_red lor bg_lightblack
let bg_green          = caml_bg_green          ()
let bg_lightgreen     = bg_green lor bg_lightblack
let bg_blue           = caml_bg_blue           ()
let bg_lightblue      = bg_blue lor bg_lightblack
let bg_magenta        = bg_red lor bg_blue
let bg_lightmagenta   = bg_red lor bg_blue lor bg_lightblack
let bg_cyan           = bg_green lor bg_blue
let bg_lightcyan      = bg_green lor bg_blue lor bg_lightblack
let bg_brown          = bg_red lor bg_green
let bg_yellow         = bg_red lor bg_green lor bg_lightblack
let bg_gray           = bg_red lor bg_green lor bg_blue
let bg_white          = bg_red lor bg_green lor bg_blue lor bg_lightblack


(** Sets the current color of the console text *)
let set_console_text_attribute handle attrib =
	if caml_set_console_text_attribute handle attrib then (
		()
	) else (
		raise (Console_error ("set_console_text_attribute", caml_get_last_error ()))
	)

(** Info about the console *)
let get_console_screen_buffer_info handle =
	let (a,b) = caml_get_console_screen_buffer_info handle in
	if a then (
		b
	) else (
		raise (Console_error ("get_console_screen_buffer_info", caml_get_last_error ()))
	)

(** Gets the current color of the console text *)
let get_console_text_attribute handle =
	let (a,b) = caml_get_console_text_attribute handle in
	if a then (
		b
	) else (
		raise (Console_error ("get_console_text_attribute", caml_get_last_error ()))
	)

(** Sets the current position of the cursor in the console. If you go out of bounds you will get a Console_error ("set_console_cursor_position",87) *)
let set_console_cursor_position handle xy =
	if caml_set_console_cursor_position handle xy then (
		()
	) else (
		raise (Console_error ("set_console_cursor_position", caml_get_last_error ()))
	)

(** Writes the specified string to the console *)
let write handle str =
	let (a,b) = caml_write handle str in
	if a then (
		b
	) else (
		raise (Console_error ("write", caml_get_last_error ()))
	)

let rec really_write handle str =
	let written = write handle str in
	if written < String.length str then (
		really_write handle (String.sub str written (String.length str - written))
	)
;;
