# Change this if compiling on Unixy OS (or run "make OBJ_EXT=.o")
OBJ_EXT=.obj

controller: controllerinclude.cmx ratecontrol.cmx pass1.cmx pass2.cmx controller.ml
	ocamlopt -o controller -thread -I ../common unix.cmxa str.cmxa threads.cmxa xml-light.cmxa extLib.cmxa pack.cmx rbtree.cmx types.cmx ../common/c-optimizations$(OBJ_EXT) optimizations.cmx c-console$(OBJ_EXT) console.cmx controllerinclude.cmx list2.cmx net.cmx huff.cmx matroska.cmx ratecontrol.cmx pass1.cmx pass2.cmx controller.ml

pass1.cmx: c-console$(OBJ_EXT) console.cmx controllerinclude.cmx pass1.ml
	ocamlopt -c -thread -I ../common unix.cmxa str.cmxa threads.cmxa xml-light.cmxa pack.cmx types.cmx c-console$(OBJ_EXT) console.cmx controllerinclude.cmx list2.cmx net.cmx huff.cmx pass1.ml

pass2.cmx: ratecontrol.cmx c-console$(OBJ_EXT) console.cmx controllerinclude.cmx pass2.ml
	ocamlopt -c -thread -I ../common unix.cmxa str.cmxa threads.cmxa xml-light.cmxa pack.cmx types.cmx c-console$(OBJ_EXT) console.cmx controllerinclude.cmx list2.cmx net.cmx huff.cmx rbtree.cmx matroska.cmx ratecontrol.cmx pass2.ml

ratecontrol.cmx: c-console$(OBJ_EXT) console.cmx controllerinclude.cmx ratecontrol.ml
	ocamlopt -c -thread -I ../common unix.cmxa str.cmxa threads.cmxa xml-light.cmxa pack.cmx types.cmx c-console$(OBJ_EXT) console.cmx controllerinclude.cmx list2.cmx net.cmx rbtree.cmx ratecontrol.ml


controllerinclude.cmx: c-console$(OBJ_EXT) console.cmx controllerinclude.ml
	ocamlopt -c -thread -I ../common unix.cmxa str.cmxa threads.cmxa xml-light.cmxa pack.cmx types.cmx c-console$(OBJ_EXT) console.cmx controllerinclude.ml


# C!
console.cmx: c-console$(OBJ_EXT) console.ml
	ocamlopt -c c-console$(OBJ_EXT) console.ml

c-console$(OBJ_EXT): c-console.c
	ocamlopt -c c-console.c

#optimizations.cmx: c-optimizations$(OBJ_EXT) optimizations.ml
#	ocamlopt -c c-optimizations$(OBJ_EXT) optimizations.ml
#
#c-optimizations$(OBJ_EXT): c-optimizations.c
#	ocamlopt -c c-optimizations.c


#matroskastream.cmx: matroskastream.ml
#	ocamlopt -i -thread -I ../common unix.cmxa threads.cmxa types.cmx matroskastream.ml
#	ocamlopt -c -thread -I ../common unix.cmxa threads.cmxa types.cmx matroskastream.ml

clean:
	rm -rf ./*.exe
	rm -rf ./*.obj
	rm -rf ./*.cmi
	rm -rf ./*.cmx
	rm -rf ./*.cmo
	rm -rf ./*.cma
	rm -rf ./*.cmxa
	rm -rf ./*.lib
	rm -rf ./*.asm
