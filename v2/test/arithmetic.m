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
 set message=":"_message_":"
 quit message_n1

add(n1,n2)
 quit n1+n2

sub(n1,n2)
 quit n1-n2

noop()
 quit

param32(p1,p2,p3,p4,p5,p6,p7,p8,p9,p10,p11,p12,p13,p14,p15,p16,p17,p18,p19,p20,p21,p22,p23,p24,p25,p26,p27,p28,p29,p30,p31,p32)
 quit p1_","_p2_","_p3_","_p4_","_p5_","_p6_","_p7_","_p8_","_p9_","_p10_","_p11_","_p12_","_p13_","_p14_","_p15_","_p16_","_p17_","_p18_","_p19_","_p20_","_p21_","_p22_","_p23_","_p24_","_p25_","_p26_","_p27_","_p28_","_p29_","_p30_","_p31_","_p32
