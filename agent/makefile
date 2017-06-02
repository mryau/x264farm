OBJ_EXT=.obj


agent: stringbuffer.cmx agentinclude.cmx agent.ml
	ocamlopt -o agent -thread -I ../common unix.cmxa str.cmxa threads.cmxa xml-light.cmxa pack.cmx types.cmx ../common/c-optimizations$(OBJ_EXT) optimizations.cmx agentinclude.cmx list2.cmx huff.cmx net.cmx matroska.cmx stringbuffer.cmx agent.ml

prodcons.cmx: prodcons.ml
	ocamlopt -c -thread -I ../common unix.cmxa threads.cmxa prodcons.ml

stringbuffer.cmx: stringbuffer.ml
	ocamlopt -c -thread unix.cmxa threads.cmxa stringbuffer.ml

#pass1.cmx: pass1.ml
#	ocamlopt -c -thread -I ../common unix.cmxa str.cmxa threads.cmxa xml-light.cmxa pack.cmx types.cmx list2.cmx net.cmx pass1.ml
#
#pass2.cmx: pass2.ml
#	ocamlopt -c -thread -I ../common unix.cmxa str.cmxa threads.cmxa xml-light.cmxa pack.cmx types.cmx list2.cmx net.cmx pass2.ml

agentinclude.cmx: agentinclude.ml
	ocamlopt -c -thread unix.cmxa threads.cmxa agentinclude.ml

clean:
	rm -rf ./agent.exe
	rm -rf ./*.obj
	rm -rf ./*.cmi
	rm -rf ./*.cmx
	rm -rf ./*.cmo
	rm -rf ./*.cma
	rm -rf ./*.cmxa
	rm -rf ./*.lib
	rm -rf ./*.asm
