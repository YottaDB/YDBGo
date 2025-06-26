;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
;; Copyright (c) 2025 YottaDB LLC and;or its subsidiaries.
;; All rights reserved.
;;
;;	This source code contains the intellectual property
;;	of its copyright holder(s), and is made available
;;	under a license.  If you do not know the terms of
;;	the license, please stop and do not read further.
;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

;Arithmetic routine to test M calls from YDBLua

addVerbose(message,n1,n2,n3)
 set n1=n1+n2+n3
 set message=message_":"
 quit message_n1

add(n1,n2)
 quit n1+n2

sub(n1,n2)
 quit n1-n2

noop()
 quit
