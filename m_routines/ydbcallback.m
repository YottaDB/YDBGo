;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;								;
; Copyright (c) 2018 YottaDB LLC and/or its subsidiaries.	;
; All rights reserved.						;
;								;
;	This source code contains the intellectual property	;
;	of its copyright holder(s), and is made available	;
;	under a license.  If you do not know the terms of	;
;	the license, please stop and do not read further.	;
;								;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

run(localname)
    set fncall=""
    ; Verify localname != ""
    set fn=$G(@localname@("rtn"))
    ; Verify fn != ""
    set expectReturn=$G(@localname@("return"))
    set fncall=fn_"("
    set index=0
    for  quit:$d(@localname@("args",index))=0  set:index fncall=fncall_"," set fncall=fncall_""""_@localname@("args",index)_"""" if $I(index)
    set fncall=fncall_")"
    if expectReturn xecute "SET "_localname_"(""retval"")=$$"_fncall
    else  xecute "DO "_fncall SET @localname@("retval")=""

