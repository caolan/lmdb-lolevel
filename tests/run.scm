(use test test-generative data-generators lmdb-lolevel posix)


;; LMDB does not work RDONLY on OpenBSD and should be used in MDB_WRITEMAP mode
(define openbsd (string=? (car (system-information)) "OpenBSD"))
(define env-flags (if openbsd MDB_WRITEMAP 0))


(define (clear-testdb)
  (when (file-exists? "testdb")
    (delete-directory "testdb" #t))
  (create-directory "testdb"))

(test-group "general"
  (test-assert (string? (mdb-version-string)))
  (test-assert (vector? (mdb-version)))
  (test "MDB_NOTFOUND: No matching key/data pair found"
	(mdb-strerror MDB_NOTFOUND)))

(test-group "environment"
  (let ((env (mdb-env-create)))
    (test "mdb-env-open for missing directory"
	  'fail
	  (condition-case
	      (mdb-env-open env "missingdb" env-flags
			    (bitwise-ior perm/irusr
					 perm/iwusr
					 perm/irgrp
					 perm/iroth))
	    ((exn lmdb ENOENT) 'fail)))
    (mdb-env-close env)))

(test-group "basic put/get in same transaction"
  (clear-testdb)
  (let ((env (mdb-env-create)))
    (mdb-env-open env "testdb" env-flags
		  (bitwise-ior perm/irusr perm/iwusr perm/irgrp perm/iroth))
    (let* ((txn (mdb-txn-begin env #f 0))
	   (dbi (mdb-dbi-open txn #f 0)))
      (mdb-put txn dbi (string->blob "foo") (string->blob "bar") 0)
      (test (string->blob "bar") (mdb-get txn dbi (string->blob "foo")))
      (mdb-txn-commit txn))
    (mdb-env-close env)))

(test-group "basic put/get in separate transactions"
  (clear-testdb)
  (let ((env (mdb-env-create)))
    (mdb-env-open env "testdb" env-flags
		  (bitwise-ior perm/irusr perm/iwusr perm/irgrp perm/iroth))
    (let* ((txn (mdb-txn-begin env #f 0))
	   (dbi (mdb-dbi-open txn #f 0)))
      (mdb-put txn dbi (string->blob "foo") (string->blob "bar") 0)
      (mdb-txn-commit txn))
    (let* ((txn (mdb-txn-begin env #f 0))
	   (dbi (mdb-dbi-open txn #f 0)))
      (test (string->blob "bar") (mdb-get txn dbi (string->blob "foo")))
      (mdb-txn-commit txn))
    (mdb-env-close env)))

(test-group "not found error condition"
  (clear-testdb)
  (let ((env (mdb-env-create)))
    (mdb-env-open env "testdb" env-flags
		  (bitwise-ior perm/irusr perm/iwusr perm/irgrp perm/iroth))
    (let* ((txn (mdb-txn-begin env #f 0))
	   (dbi (mdb-dbi-open txn #f 0)))
      (test 'not-found
	    (condition-case (mdb-get txn dbi (string->blob "foo"))
	      ((exn lmdb MDB_NOTFOUND) 'not-found)))
      (mdb-txn-commit txn))
    (mdb-env-close env)))

(test-group "abort transaction"
  (clear-testdb)
  (let ((env (mdb-env-create)))
    (mdb-env-open env "testdb" env-flags
		  (bitwise-ior perm/irusr perm/iwusr perm/irgrp perm/iroth))
    (let* ((txn (mdb-txn-begin env #f 0))
	   (dbi (mdb-dbi-open txn #f 0)))
      (mdb-put txn dbi (string->blob "foo") (string->blob "one") 0)
      (mdb-txn-commit txn))
    (let* ((txn (mdb-txn-begin env #f 0))
	   (dbi (mdb-dbi-open txn #f 0)))
      (mdb-put txn dbi (string->blob "bar") (string->blob "two") 0)
      (mdb-txn-abort txn))
    (let* ((txn (mdb-txn-begin env #f 0))
	   (dbi (mdb-dbi-open txn #f 0)))
      (test (string->blob "one") (mdb-get txn dbi (string->blob "foo")))
      (test 'not-found
	    (condition-case (mdb-get txn dbi (string->blob "bar"))
	      ((exn lmdb MDB_NOTFOUND) 'not-found)))
      (mdb-txn-commit txn))
    (mdb-env-close env)))

(test-group "check invalid pointer tags"
  (clear-testdb)
  (let ((env (mdb-env-create)))
    (mdb-env-open env "testdb" env-flags
		  (bitwise-ior perm/irusr perm/iwusr perm/irgrp perm/iroth))
    (let* ((txn (mdb-txn-begin env #f 0))
	   (dbi (mdb-dbi-open txn #f 0)))
      (test "mdb-env-open"
	    'bad-arg
	    (condition-case (mdb-env-open txn (string->blob "testdb2") 0 0)
	      ((exn type) 'bad-arg)))
       (test "mdb-env-copy"
	    'bad-arg
	    (condition-case (mdb-env-copy txn (string->blob "testdb2"))
	      ((exn type) 'bad-arg)))
       (test "mdb-env-copyfd"
	    'bad-arg
	    (condition-case (mdb-env-copyfd txn 0)
	      ((exn type) 'bad-arg)))
       (test "mdb-env-copy2"
	    'bad-arg
	    (condition-case (mdb-env-copy2 txn (string->blob "testdb2") 0)
	      ((exn type) 'bad-arg)))
      (test "mdb-env-close"
	    'bad-arg
	    (condition-case (mdb-env-close txn)
	      ((exn type) 'bad-arg)))
      (test "mdb-txn-begin"
	    'bad-arg
	    (condition-case (mdb-txn-begin txn #f 0)
	      ((exn type) 'bad-arg)))
      (test "mdb-txn-commit"
	    'bad-arg
	    (condition-case (mdb-txn-commit env)
	      ((exn type) 'bad-arg)))
      (test "mdb-txn-abort"
	    'bad-arg
	    (condition-case (mdb-txn-abort env)
	      ((exn type) 'bad-arg)))
      (test "mdb-dbi-open"
	    'bad-arg
	    (condition-case (mdb-dbi-open env #f 0)
	      ((exn type) 'bad-arg)))
      (test "mdb-dbi-close"
	    'bad-arg
	    (condition-case (mdb-dbi-close txn (string->blob "test"))
	      ((exn type) 'bad-arg)))
      (test "mdb-put"
	    'bad-arg
	    (condition-case (mdb-put env
				     dbi
				     (string->blob "foo")
				     (string->blob "bar")
				     0)
	      ((exn type) 'bad-arg)))
      (test "mdb-get"
	    'bad-arg
	    (condition-case (mdb-get env dbi (string->blob "foo"))
	      ((exn type) 'bad-arg)))
      (test "mdb-del"
	    'bad-arg
	    (condition-case (mdb-del env dbi (string->blob "foo"))
	      ((exn type) 'bad-arg)))
      (mdb-txn-abort txn))
    (mdb-env-close env)))

(test-group "copy environment to filename"
  (clear-testdb)
  (let ((env (mdb-env-create)))
    (mdb-env-open env "testdb" env-flags
		  (bitwise-ior perm/irusr perm/iwusr perm/irgrp perm/iroth))
    (let* ((txn (mdb-txn-begin env #f 0))
	   (dbi (mdb-dbi-open txn #f 0)))
      (mdb-put txn dbi (string->blob "foo") (string->blob "one") 0)
      (mdb-put txn dbi (string->blob "bar") (string->blob "two") 0)
      (mdb-txn-commit txn))
    (when (file-exists? "testdb2")
      (delete-directory "testdb2" #t))
    (create-directory "testdb2")
    (mdb-env-copy env "testdb2")
    (mdb-env-close env))
  (let ((env (mdb-env-create)))
    (mdb-env-open env "testdb2" env-flags
		  (bitwise-ior perm/irusr perm/iwusr perm/irgrp perm/iroth))
    (let* ((txn (mdb-txn-begin env #f 0))
	   (dbi (mdb-dbi-open txn #f 0)))
      (test (string->blob "one") (mdb-get txn dbi (string->blob "foo")))
      (test (string->blob "two") (mdb-get txn dbi (string->blob "bar")))
      (mdb-txn-commit txn))
    (mdb-env-close env)))

(test-group "copy environment to fd"
  (clear-testdb)
  (let ((env (mdb-env-create)))
    (mdb-env-open env "testdb" env-flags
		  (bitwise-ior perm/irusr perm/iwusr perm/irgrp perm/iroth))
    (let* ((txn (mdb-txn-begin env #f 0))
	   (dbi (mdb-dbi-open txn #f 0)))
      (mdb-put txn dbi (string->blob "foo") (string->blob "one") 0)
      (mdb-put txn dbi (string->blob "bar") (string->blob "two") 0)
      (mdb-txn-commit txn))
    (when (file-exists? "testdb2")
      (delete-directory "testdb2" #t))
    (create-directory "testdb2")
    (let ((fd (file-open "testdb2/data.mdb"
			 (bitwise-ior open/wronly open/creat)
			 (bitwise-ior perm/irusr perm/iwusr perm/irgrp perm/iroth))))
			 
      (mdb-env-copyfd env fd)
      (file-close fd))
    (mdb-env-close env))
  (let ((env (mdb-env-create)))
    (mdb-env-open env "testdb2" env-flags
		  (bitwise-ior perm/irusr perm/iwusr perm/irgrp perm/iroth))
    (let* ((txn (mdb-txn-begin env #f 0))
	   (dbi (mdb-dbi-open txn #f 0)))
      (test (string->blob "one") (mdb-get txn dbi (string->blob "foo")))
      (test (string->blob "two") (mdb-get txn dbi (string->blob "bar")))
      (mdb-txn-commit txn))
    (mdb-env-close env)))

(test-group "copy environment to filename with compaction"
  (clear-testdb)
  (let ((env (mdb-env-create)))
    (mdb-env-open env "testdb" env-flags
		  (bitwise-ior perm/irusr perm/iwusr perm/irgrp perm/iroth))
    (let* ((txn (mdb-txn-begin env #f 0))
	   (dbi (mdb-dbi-open txn #f 0)))
      (mdb-put txn dbi (string->blob "foo") (string->blob "one") 0)
      (mdb-put txn dbi (string->blob "bar") (string->blob "two") 0)
      (mdb-put txn dbi (string->blob "baz") (string->blob "three") 0)
      (mdb-put txn dbi (string->blob "qux") (string->blob "four") 0)
      (mdb-txn-commit txn))
    (let* ((txn (mdb-txn-begin env #f 0))
	   (dbi (mdb-dbi-open txn #f 0)))
      (mdb-del txn dbi (string->blob "baz"))
      (mdb-del txn dbi (string->blob "qux"))
      (mdb-txn-commit txn))
    (when (file-exists? "testdb2")
      (delete-directory "testdb2" #t))
    (create-directory "testdb2")
    (mdb-env-copy2 env "testdb2" MDB_CP_COMPACT)
    (mdb-env-close env))
  (let ((env (mdb-env-create)))
    (mdb-env-open env "testdb2" env-flags
		  (bitwise-ior perm/irusr perm/iwusr perm/irgrp perm/iroth))
    (let* ((txn (mdb-txn-begin env #f 0))
	   (dbi (mdb-dbi-open txn #f 0)))
      (test (string->blob "one") (mdb-get txn dbi (string->blob "foo")))
      (test (string->blob "two") (mdb-get txn dbi (string->blob "bar")))
      (mdb-txn-commit txn))
    (mdb-env-close env))
  (unless (= (bitwise-and MDB_WRITEMAP env-flags) MDB_WRITEMAP)
    (test-assert (> (file-size "testdb/data.mdb")
 		    (file-size "testdb2/data.mdb")))))

(test-group "copy environment to fd with compaction"
  (clear-testdb)
  (let ((env (mdb-env-create)))
    (mdb-env-open env "testdb" env-flags
		  (bitwise-ior perm/irusr perm/iwusr perm/irgrp perm/iroth))
    (let* ((txn (mdb-txn-begin env #f 0))
	   (dbi (mdb-dbi-open txn #f 0)))
      (mdb-put txn dbi (string->blob "foo") (string->blob "one") 0)
      (mdb-put txn dbi (string->blob "bar") (string->blob "two") 0)
      (mdb-put txn dbi (string->blob "baz") (string->blob "three") 0)
      (mdb-put txn dbi (string->blob "qux") (string->blob "four") 0)
      (mdb-txn-commit txn))
    (let* ((txn (mdb-txn-begin env #f 0))
	   (dbi (mdb-dbi-open txn #f 0)))
      (mdb-del txn dbi (string->blob "baz"))
      (mdb-del txn dbi (string->blob "qux"))
      (mdb-txn-commit txn))
    (when (file-exists? "testdb2")
      (delete-directory "testdb2" #t))
    (create-directory "testdb2")
    (let ((fd (file-open "testdb2/data.mdb"
			 (bitwise-ior open/wronly open/creat)
			 (bitwise-ior perm/irusr perm/iwusr perm/irgrp perm/iroth))))
			 
      (mdb-env-copyfd2 env fd MDB_CP_COMPACT)
      (file-close fd))
    (mdb-env-close env))
  (let ((env (mdb-env-create)))
    (mdb-env-open env "testdb2" env-flags
		  (bitwise-ior perm/irusr perm/iwusr perm/irgrp perm/iroth))
    (let* ((txn (mdb-txn-begin env #f 0))
	   (dbi (mdb-dbi-open txn #f 0)))
      (test (string->blob "one") (mdb-get txn dbi (string->blob "foo")))
      (test (string->blob "two") (mdb-get txn dbi (string->blob "bar")))
      (mdb-txn-commit txn))
    (mdb-env-close env))
  (unless (= (bitwise-and MDB_WRITEMAP env-flags) MDB_WRITEMAP)
    (test-assert (> (file-size "testdb/data.mdb")
                    (file-size "testdb2/data.mdb")))))

(test-group "mdb-env-stat"
  (clear-testdb)
  (let ((env (mdb-env-create)))
    (mdb-env-open env "testdb" env-flags
		  (bitwise-ior perm/irusr perm/iwusr perm/irgrp perm/iroth))
    (let* ((txn (mdb-txn-begin env #f 0))
	   (dbi (mdb-dbi-open txn #f 0)))
      (mdb-put txn dbi (string->blob "foo") (string->blob "one") 0)
      (mdb-put txn dbi (string->blob "bar") (string->blob "two") 0)
      (mdb-txn-commit txn))
    (let ((stat (mdb-env-stat env)))
      (test-assert (number? (mdb-stat-psize stat)))
      (test-assert (number? (mdb-stat-depth stat)))
      (test-assert (number? (mdb-stat-branch-pages stat)))
      (test-assert (number? (mdb-stat-leaf-pages stat)))
      (test-assert (number? (mdb-stat-overflow-pages stat)))
      (test-assert (number? (mdb-stat-entries stat)))
      (test 2 (mdb-stat-entries stat)))
    (mdb-env-close env)))

(test-group "mdb-env-info"
  (clear-testdb)
  (let ((env (mdb-env-create)))
    (mdb-env-open env "testdb" env-flags
		  (bitwise-ior perm/irusr perm/iwusr perm/irgrp perm/iroth))
    (let* ((txn (mdb-txn-begin env #f 0))
	   (dbi (mdb-dbi-open txn #f 0)))
      (mdb-put txn dbi (string->blob "foo") (string->blob "one") 0)
      (mdb-put txn dbi (string->blob "bar") (string->blob "two") 0)
      (mdb-txn-commit txn))
    (let ((info (mdb-env-info env)))
      (test-assert (not (mdb-envinfo-mapaddr info))) ;; NULL pointer
      (test-assert (number? (mdb-envinfo-mapsize info)))
      (test-assert (number? (mdb-envinfo-last-pgno info)))
      (test-assert (number? (mdb-envinfo-last-txnid info)))
      (test-assert (number? (mdb-envinfo-maxreaders info)))
      (test-assert (number? (mdb-envinfo-numreaders info))))
    (mdb-env-close env)))

(test-group "mdb-env-sync"
  (clear-testdb)
  (let ((env (mdb-env-create)))
    (mdb-env-open env "testdb" (bitwise-ior env-flags MDB_NOSYNC)
		  (bitwise-ior perm/irusr perm/iwusr perm/irgrp perm/iroth))
    (let* ((txn (mdb-txn-begin env #f 0))
	   (dbi (mdb-dbi-open txn #f 0)))
      (mdb-put txn dbi (string->blob "foo") (string->blob "one") 0)
      (mdb-put txn dbi (string->blob "bar") (string->blob "two") 0)
      (mdb-txn-commit txn))
    ;; no asserts, just checking this runs without an exception for now
    (mdb-env-sync env 1)
    (mdb-env-close env)))

(test-group "mdb-env-get-flags / mdb-env-set-flags"
  (clear-testdb)
  (let ((env (mdb-env-create)))
    (mdb-env-open env "testdb" 0
		  (bitwise-ior perm/irusr perm/iwusr perm/irgrp perm/iroth))
    (test-assert (not (= (bitwise-and MDB_NOSYNC (mdb-env-get-flags env))
			 MDB_NOSYNC)))
    (mdb-env-set-flags env MDB_NOSYNC 1)
    (test-assert (= (bitwise-and MDB_NOSYNC (mdb-env-get-flags env))
		    MDB_NOSYNC))
    (mdb-env-set-flags env MDB_NOSYNC 0)
    (test-assert (not (= (bitwise-and MDB_NOSYNC (mdb-env-get-flags env))
			 MDB_NOTLS)))
    (mdb-env-close env)))

(test-group "mdb-env-get-path"
  (clear-testdb)
  (let ((env (mdb-env-create)))
    (mdb-env-open env "testdb" env-flags
		  (bitwise-ior perm/irusr perm/iwusr perm/irgrp perm/iroth))
    (test "testdb" (mdb-env-get-path env))
    (mdb-env-close env)))

(test-group "mdb-env-get-fd"
  (clear-testdb)
  (let ((env (mdb-env-create)))
    (mdb-env-open env "testdb" env-flags
		  (bitwise-ior perm/irusr perm/iwusr perm/irgrp perm/iroth))
    (test-assert (number? (mdb-env-get-fd env)))
    (mdb-env-close env)))

(test-group "mdb-env-set-mapsize"
  (clear-testdb)
  (let ((env (mdb-env-create)))
    (mdb-env-open env "testdb" env-flags
		  (bitwise-ior perm/irusr perm/iwusr perm/irgrp perm/iroth))
    ;; no asserts, just checking this runs without an exception for now
    (mdb-env-set-mapsize env (* 2 10485760))
    (mdb-env-close env)))

(test-group "mdb-env-get-maxreaders / mdb-env-set-maxreaders"
  (clear-testdb)
  (let ((env (mdb-env-create)))
    (let ((n (mdb-env-get-maxreaders env)))
      (mdb-env-set-maxreaders env (- n 1))
      (test (- n 1) (mdb-env-get-maxreaders env)))
    (mdb-env-close env)))

(test-group "mdb-env-set-maxdbs"
  (clear-testdb)
  (let ((env (mdb-env-create)))
    (mdb-env-set-maxdbs env 2)
    (mdb-env-open env "testdb" env-flags
		  (bitwise-ior perm/irusr perm/iwusr perm/irgrp perm/iroth))
    ;; this would normally cause MDB_DBS_FULL error
    (let* ((txn (mdb-txn-begin env #f 0))
	   (dbi1 (mdb-dbi-open txn "one" MDB_CREATE))
	   (dbi2 (mdb-dbi-open txn "two" MDB_CREATE)))
      (mdb-put txn dbi1 (string->blob "foo") (string->blob "111") 0)
      (mdb-put txn dbi2 (string->blob "bar") (string->blob "222") 0)
      (mdb-txn-commit txn))
    (mdb-env-close env)))

(test-group "mdb-env-get-maxkeysize"
  (let ((env (mdb-env-create)))
    (test 511 (mdb-env-get-maxkeysize env))
    (mdb-env-close env)))

(test-group "mdb-txn-env"
  (clear-testdb)
  (let ((env (mdb-env-create)))
    (mdb-env-open env "testdb" (bitwise-ior env-flags MDB_NOSYNC)
		  (bitwise-ior perm/irusr perm/iwusr perm/irgrp perm/iroth))
    (let ((txn (mdb-txn-begin env #f 0)))
      (test env (mdb-txn-env txn))
      (mdb-txn-commit txn))
    (mdb-env-close env)))

;; this feature only available in LMDB 0.9.15+
;; debian jessie (current stable) only has LMDB 0.9.14
(define mdb-txn-id-available
  (every (cut apply (cut >= <> <>) <>)
         (zip (vector->list (mdb-version)) '(0 9 15))))

(when mdb-txn-id-available
  (test-group "mdb-txn-id"
    (clear-testdb)
    (let ((env (mdb-env-create)))
      (mdb-env-open env "testdb" (bitwise-ior env-flags MDB_NOSYNC)
  		  (bitwise-ior perm/irusr perm/iwusr perm/irgrp perm/iroth))
      (let ((txn (mdb-txn-begin env #f 0)))
        (test-assert (number? (mdb-txn-id txn)))
        (mdb-txn-commit txn))
      (mdb-env-close env))))

(test-group "mdb-txn-reset / mdb-txn-renew"
  (clear-testdb)
  (let ((env (mdb-env-create)))
    (mdb-env-open env "testdb" (bitwise-ior env-flags MDB_NOSYNC)
		  (bitwise-ior perm/irusr perm/iwusr perm/irgrp perm/iroth))
    (let* ((txn (mdb-txn-begin env #f 0))
	   (dbi (mdb-dbi-open txn #f 0)))
      (mdb-put txn dbi (string->blob "foo") (string->blob "one") 0)
      (mdb-put txn dbi (string->blob "bar") (string->blob "two") 0)
      (mdb-txn-commit txn))
    (let* ((txn (mdb-txn-begin env #f 0))
	   (dbi (mdb-dbi-open txn #f 0)))
      (test (string->blob "one") (mdb-get txn dbi (string->blob "foo")))
      (mdb-txn-reset txn)
      (mdb-txn-renew txn)
      (test (string->blob "one") (mdb-get txn dbi (string->blob "foo")))
      )
    (mdb-env-close env)))

(test-group "mdb-stat"
  (clear-testdb)
  (let ((env (mdb-env-create)))
    (mdb-env-open env "testdb" env-flags
		  (bitwise-ior perm/irusr perm/iwusr perm/irgrp perm/iroth))
    (let* ((txn (mdb-txn-begin env #f 0))
	   (dbi (mdb-dbi-open txn #f 0))
	   (stat (mdb-stat txn dbi)))
      (test-assert (number? (mdb-stat-psize stat)))
      (test-assert (number? (mdb-stat-depth stat)))
      (test-assert (number? (mdb-stat-branch-pages stat)))
      (test-assert (number? (mdb-stat-leaf-pages stat)))
      (test-assert (number? (mdb-stat-overflow-pages stat)))
      (test-assert (number? (mdb-stat-entries stat)))
      (test 0 (mdb-stat-entries stat))
      (mdb-txn-commit txn))
    (mdb-env-close env)))

(test-group "mdb-dbi-flags"
  (clear-testdb)
  (let ((env (mdb-env-create)))
    (mdb-env-open env "testdb" env-flags
		  (bitwise-ior perm/irusr perm/iwusr perm/irgrp perm/iroth))
    (let* ((txn (mdb-txn-begin env #f 0))
	   (dbi (mdb-dbi-open txn #f MDB_REVERSEKEY)))
      (test MDB_REVERSEKEY
	    (bitwise-and MDB_REVERSEKEY (mdb-dbi-flags txn dbi)))
      (mdb-txn-commit txn))
    (mdb-env-close env)))

(test-group "mdb-drop"
  (clear-testdb)
  (let ((env (mdb-env-create)))
    (mdb-env-set-maxdbs env 2)
    (mdb-env-open env "testdb" env-flags
		  (bitwise-ior perm/irusr perm/iwusr perm/irgrp perm/iroth))
    (let* ((txn (mdb-txn-begin env #f 0))
	   (dbi1 (mdb-dbi-open txn "one" MDB_CREATE))
	   (dbi2 (mdb-dbi-open txn "two" MDB_CREATE)))
      (mdb-put txn dbi1 (string->blob "foo") (string->blob "111") 0)
      (mdb-put txn dbi2 (string->blob "bar") (string->blob "222") 0)
      (mdb-txn-commit txn))
    (let* ((txn (mdb-txn-begin env #f 0))
	   (dbi1 (mdb-dbi-open txn "one" 0))
	   (dbi2 (mdb-dbi-open txn "two" 0)))
      (mdb-drop txn dbi1 0)
      (mdb-drop txn dbi2 1)
      (mdb-txn-commit txn))
    (let* ((txn (mdb-txn-begin env #f 0))
	   (dbi1 (mdb-dbi-open txn "one" 0)))
      ;; empty database
      (test 'not-found
	    (condition-case (mdb-get txn dbi1 (string->blob "foo"))
	      ((exn lmdb MDB_NOTFOUND) 'not-found)))
      ;; deleted database
      (test 'not-found
	    (condition-case (mdb-dbi-open txn "two" 0)
	      ((exn lmdb MDB_NOTFOUND) 'not-found)))
      (mdb-txn-commit txn))
    (mdb-env-close env)))

;; custom sort/comparison functions are only tested
;; if the test suite is compiled with example_cmp.c
(cond-expand
  (compiling
   (foreign-declare "#include <lmdb.h>")
   (foreign-declare
    "int example_cmp(const MDB_val *a, const MDB_val *b) {
       char ca = ((char *)a->mv_data)[1];
       char cb = ((char *)b->mv_data)[1];
       if (ca > cb) {
         return 1;
       }
       else if (ca < cb) {
         return -1;
       }
       return 0;
     }")

   (define example_cmp
     (foreign-value "&example_cmp" c-pointer))

   (test-group "set compare"
     (clear-testdb)
     (let ((env (mdb-env-create)))
       (mdb-env-open env "testdb" env-flags
		     (bitwise-ior perm/irusr perm/iwusr perm/irgrp perm/iroth))
       (let* ((txn (mdb-txn-begin env #f 0))
	      (dbi (mdb-dbi-open txn #f 0))
	      (cursor (mdb-cursor-open txn dbi)))
	 (mdb-set-compare txn dbi example_cmp)
	 (mdb-put txn dbi (string->blob "ab") (string->blob "one") 0)
	 (mdb-put txn dbi (string->blob "ca") (string->blob "two") 0)
	 (mdb-cursor-get cursor #f #f MDB_FIRST)
	 (test "ca" (blob->string (mdb-cursor-key cursor)))
	 (mdb-cursor-get cursor #f #f MDB_NEXT)
	 (test "ab" (blob->string (mdb-cursor-key cursor)))
	 (mdb-cursor-close cursor)
	 (mdb-txn-commit txn))
       (mdb-env-close env)))
  
   (test-group "set dupsort"
     (clear-testdb)
     (let ((env (mdb-env-create)))
       (mdb-env-open env "testdb" env-flags
		     (bitwise-ior perm/irusr perm/iwusr perm/irgrp perm/iroth))
       (let* ((txn (mdb-txn-begin env #f 0))
	      (dbi (mdb-dbi-open txn #f MDB_DUPSORT))
	      (cursor (mdb-cursor-open txn dbi)))
	 (mdb-set-dupsort txn dbi example_cmp)
	 (mdb-put txn dbi (string->blob "foo") (string->blob "ab") 0)
	 (mdb-put txn dbi (string->blob "foo") (string->blob "ca") 0)
	 (mdb-cursor-get cursor #f #f MDB_FIRST)
	 (test "ca" (blob->string (mdb-cursor-data cursor)))
	 (mdb-cursor-get cursor #f #f MDB_NEXT)
	 (test "ab" (blob->string (mdb-cursor-data cursor)))
	 (mdb-cursor-close cursor)
	 (mdb-txn-commit txn))
       (mdb-env-close env))))
   (else #f))

(test-group "mdb-del"
  (clear-testdb)
  (let ((env (mdb-env-create)))
    (mdb-env-open env "testdb" env-flags
		  (bitwise-ior perm/irusr perm/iwusr perm/irgrp perm/iroth))
    (let* ((txn (mdb-txn-begin env #f 0))
	   (dbi (mdb-dbi-open txn #f 0)))
      (mdb-put txn dbi (string->blob "foo") (string->blob "one") 0)
      (mdb-put txn dbi (string->blob "bar") (string->blob "two") 0)
      (mdb-txn-commit txn))
    (let* ((txn (mdb-txn-begin env #f 0))
	   (dbi (mdb-dbi-open txn #f 0)))
      (test (string->blob "one") (mdb-get txn dbi (string->blob "foo")))
      (test (string->blob "two") (mdb-get txn dbi (string->blob "bar")))
      (mdb-del txn dbi (string->blob "bar"))
      (test (string->blob "one") (mdb-get txn dbi (string->blob "foo")))
      (test 'not-found
	    (condition-case (mdb-get txn dbi (string->blob "bar"))
	      ((exn lmdb MDB_NOTFOUND) 'not-found)))
      (mdb-txn-commit txn))
    (let* ((txn (mdb-txn-begin env #f 0))
	   (dbi (mdb-dbi-open txn #f 0)))
      (test (string->blob "one") (mdb-get txn dbi (string->blob "foo")))
      (test 'not-found
	    (condition-case (mdb-get txn dbi (string->blob "bar"))
	      ((exn lmdb MDB_NOTFOUND) 'not-found)))
      (mdb-txn-commit txn))
    (mdb-env-close env)))

(test-group "cursor txn / dbi"
  (clear-testdb)
  (let ((env (mdb-env-create)))
    (mdb-env-open env "testdb" env-flags
		  (bitwise-ior perm/irusr perm/iwusr perm/irgrp perm/iroth))
    (let* ((txn (mdb-txn-begin env #f 0))
	   (dbi (mdb-dbi-open txn #f 0))
	   (cursor (mdb-cursor-open txn dbi)))
      (test txn (mdb-cursor-txn cursor))
      (test dbi (mdb-cursor-dbi cursor))
      (mdb-cursor-close cursor)
      (mdb-txn-commit txn))
    (mdb-env-close env)))

(test-group "readonly txn and renew cursor"
  (clear-testdb)
  (let ((env (mdb-env-create)))
    (mdb-env-open env "testdb" env-flags
		  (bitwise-ior perm/irusr perm/iwusr perm/irgrp perm/iroth))
    (let* ((txn (mdb-txn-begin env #f 0))
	   (dbi (mdb-dbi-open txn #f 0)))
      (mdb-put txn dbi (string->blob "foo") (string->blob "bar") 0)
      (mdb-txn-commit txn))
    (let* ((txn (mdb-txn-begin env #f MDB_RDONLY))
	   (dbi (mdb-dbi-open txn #f 0))
	   (cursor (mdb-cursor-open txn dbi)))
        (test (string->blob "bar") (mdb-get txn dbi (string->blob "foo")))
	(mdb-txn-reset txn)
	(mdb-txn-renew txn)
        (test (string->blob "bar") (mdb-get txn dbi (string->blob "foo")))
	(mdb-txn-abort txn))
    (mdb-env-close env)))

(test-group "cursor get"
  (clear-testdb)
  (let ((env (mdb-env-create)))
    (mdb-env-open env "testdb" env-flags
		  (bitwise-ior perm/irusr perm/iwusr perm/irgrp perm/iroth))
    (let* ((txn (mdb-txn-begin env #f 0))
	   (dbi (mdb-dbi-open txn #f 0))
	   (cursor (mdb-cursor-open txn dbi)))
      (mdb-put txn dbi (string->blob "wibble") (string->blob "foo") 0)
      (mdb-put txn dbi (string->blob "wobble") (string->blob "bar") 0)
      (mdb-cursor-get cursor #f #f MDB_FIRST)
      (test (string->blob "wibble") (mdb-cursor-key cursor))
      (test (string->blob "foo") (mdb-cursor-data cursor))
      (mdb-cursor-get cursor #f #f MDB_NEXT)
      (test (string->blob "wobble") (mdb-cursor-key cursor))
      (test (string->blob "bar") (mdb-cursor-data cursor))
      (mdb-cursor-get cursor (string->blob "wibble") #f MDB_SET_KEY)
      (test (string->blob "wibble") (mdb-cursor-key cursor))
      (test (string->blob "foo") (mdb-cursor-data cursor))
      (mdb-cursor-close cursor)
      (mdb-txn-commit txn))
    (mdb-env-close env)))

(test-group "cursor put"
  (clear-testdb)
  (let ((env (mdb-env-create)))
    (mdb-env-open env "testdb" env-flags
		  (bitwise-ior perm/irusr perm/iwusr perm/irgrp perm/iroth))
    (let* ((txn (mdb-txn-begin env #f 0))
	   (dbi (mdb-dbi-open txn #f 0))
	   (cursor (mdb-cursor-open txn dbi)))
      (mdb-cursor-put cursor (string->blob "wibble") (string->blob "foo") 0)
      (mdb-cursor-put cursor (string->blob "wobble") (string->blob "bar") 0)
      (mdb-cursor-put cursor (string->blob "wubble") (string->blob "baz") 0)
      (test (string->blob "wubble") (mdb-cursor-key cursor))
      (test (string->blob "baz") (mdb-cursor-data cursor))
      (mdb-cursor-get cursor #f #f MDB_FIRST)
      (test (string->blob "wibble") (mdb-cursor-key cursor))
      (test (string->blob "foo") (mdb-cursor-data cursor))
      (mdb-cursor-close cursor)
      (mdb-txn-commit txn))
    (mdb-env-close env)))

(test-group "cursor del"
  (clear-testdb)
  (let ((env (mdb-env-create)))
    (mdb-env-open env "testdb" env-flags
		  (bitwise-ior perm/irusr perm/iwusr perm/irgrp perm/iroth))
    (let* ((txn (mdb-txn-begin env #f 0))
	   (dbi (mdb-dbi-open txn #f 0))
	   (cursor (mdb-cursor-open txn dbi)))
      (mdb-put txn dbi (string->blob "wibble") (string->blob "foo") 0)
      (mdb-put txn dbi (string->blob "wobble") (string->blob "bar") 0)
      (mdb-cursor-get cursor #f #f MDB_FIRST)
      (test (string->blob "wibble") (mdb-cursor-key cursor))
      (test (string->blob "foo") (mdb-cursor-data cursor))
      (mdb-cursor-del cursor 0)
      (mdb-cursor-get cursor #f #f MDB_FIRST)
      (test (string->blob "wobble") (mdb-cursor-key cursor))
      (test (string->blob "bar") (mdb-cursor-data cursor))
      (mdb-cursor-close cursor)
      (mdb-txn-commit txn))
    (mdb-env-close env)))

(test-group "cursor count"
  (clear-testdb)
  (let ((env (mdb-env-create)))
    (mdb-env-open env "testdb" env-flags
		  (bitwise-ior perm/irusr perm/iwusr perm/irgrp perm/iroth))
    (let* ((txn (mdb-txn-begin env #f 0))
	   (dbi (mdb-dbi-open txn #f MDB_DUPSORT))
	   (cursor (mdb-cursor-open txn dbi)))
      (mdb-put txn dbi (string->blob "wibble") (string->blob "foo") 0)
      (mdb-cursor-get cursor #f #f MDB_FIRST)
      (test 1 (mdb-cursor-count cursor))
      (mdb-put txn dbi (string->blob "wibble") (string->blob "bar") 0)
      (mdb-cursor-get cursor #f #f MDB_FIRST)
      (test 2 (mdb-cursor-count cursor))
      (mdb-cursor-close cursor)
      (mdb-txn-commit txn))
    (mdb-env-close env)))

(test-group "cmp / dcmp"
  (clear-testdb)
  (let ((env (mdb-env-create)))
    (mdb-env-open env "testdb" env-flags
		  (bitwise-ior perm/irusr perm/iwusr perm/irgrp perm/iroth))
    (let* ((txn (mdb-txn-begin env #f 0))
	   (dbi (mdb-dbi-open txn #f MDB_DUPSORT)))
      (test-assert (negative? (mdb-cmp txn dbi
				       (string->blob "aaa")
				       (string->blob "bbb"))))
      (test-assert (positive? (mdb-cmp txn dbi
				       (string->blob "xxx")
				       (string->blob "bbb"))))
      (test-assert (negative? (mdb-dcmp txn dbi
					(string->blob "aaa")
					(string->blob "bbb"))))
      (test-assert (positive? (mdb-dcmp txn dbi
					(string->blob "xxx")
					(string->blob "bbb")))))))

(test-group "reader list"
  (clear-testdb)
  (let ((env (mdb-env-create)))
    (mdb-env-open env "testdb" env-flags
		  (bitwise-ior perm/irusr perm/iwusr perm/irgrp perm/iroth))
    (let* ((txn (mdb-txn-begin env #f 0))
	   (dbi (mdb-dbi-open txn #f 0))
	   (num #f))
      (let ((str (with-output-to-string
		   (lambda ()
		     (set! num (mdb-reader-list env))))))
	(test "output written to port" "(no active readers)\n" str)
	(test "returned integer" 0 num))
      (mdb-txn-commit txn))
    (mdb-env-close env)))

(test-group "reader check"
  (clear-testdb)
  (let ((env (mdb-env-create)))
    (mdb-env-open env "testdb" env-flags
		  (bitwise-ior perm/irusr perm/iwusr perm/irgrp perm/iroth))
    (let* ((txn (mdb-txn-begin env #f 0))
	   (dbi (mdb-dbi-open txn #f 0)))
      (test 0 (mdb-reader-check env))
      (mdb-txn-commit txn))
    (mdb-env-close env)))

(test-group "throw some random data at it"
  (clear-testdb)
  (let ((env (mdb-env-create)))
    (mdb-env-open env "testdb" (bitwise-ior env-flags MDB_NOSYNC)
		  (bitwise-ior perm/irusr perm/iwusr perm/irgrp perm/iroth))
    (let ((max-key-size (mdb-env-get-maxkeysize env)))
      (parameterize
	  ((current-test-generative-iterations 100))
	(test-generative ((k (with-size (range 1 max-key-size)
					(gen-string-of (gen-char #\null #\ÿ))))
			  (v (with-size (range 1 5000)
					(gen-string-of (gen-char #\null #\ÿ)))))
	(let* ((txn (mdb-txn-begin env #f 0))
	       (dbi (mdb-dbi-open txn #f 0)))
	  (mdb-put txn dbi (string->blob k) (string->blob v) 0)
	  (test "returned data matches put"
		(string->blob v)
		(mdb-get txn dbi (string->blob k)))
	  (mdb-txn-commit txn))))
    (mdb-env-close env))))

(test-group "iterate over dupsort data"
  (clear-testdb)
  (let ((env (mdb-env-create)))
    (mdb-env-open env "testdb" env-flags
		  (bitwise-ior perm/irusr perm/iwusr perm/irgrp perm/iroth))
    (let* ((txn (mdb-txn-begin env #f 0))
	   (dbi (mdb-dbi-open txn #f MDB_DUPSORT))
	   (cursor (mdb-cursor-open txn dbi)))
      (mdb-put txn dbi (string->blob "foo") (string->blob "ab") 0)
      (mdb-put txn dbi (string->blob "foo") (string->blob "ca") 0)
      (mdb-cursor-get cursor #f #f MDB_FIRST)
      (test "ab" (blob->string (mdb-cursor-data cursor)))
      (mdb-cursor-get cursor #f #f MDB_NEXT_DUP)
      (test "ca" (blob->string (mdb-cursor-data cursor)))
      ;; calling MDB_NEXT_DUP at last duplicate data entry results in not found
      (test 'not-found
	    (condition-case (mdb-cursor-get cursor #f #f MDB_NEXT_DUP)
	      ((exn lmdb MDB_NOTFOUND) 'not-found)))
      (mdb-cursor-close cursor)
      (mdb-txn-commit txn))
    (mdb-env-close env)))

(test-exit)
