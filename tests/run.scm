(use test lmdb-lolevel posix)

(define (clear-testdb)
  (when (file-exists? "tests/testdb")
    (delete-directory "tests/testdb" #t))
  (create-directory "tests/testdb"))

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
	      (mdb-env-open env "tests/missingdb" 0
			    (bitwise-ior perm/irusr
					 perm/iwusr
					 perm/irgrp
					 perm/iroth))
	    ((exn lmdb ENOENT) 'fail)))
    (mdb-env-close env)))

(test-group "basic put/get in same transaction"
  (clear-testdb)
  (let ((env (mdb-env-create)))
    (mdb-env-open env "tests/testdb" 0
		  (bitwise-ior perm/irusr perm/iwusr perm/irgrp perm/iroth))
    (let* ((txn (mdb-txn-begin env #f 0))
	   (dbi (mdb-dbi-open txn #f 0)))
      (mdb-put txn dbi "foo" "bar" 0)
      (test "bar" (mdb-get txn dbi "foo"))
      (mdb-txn-commit txn))
    (mdb-env-close env)))

(test-group "basic put/get in separate transactions"
  (clear-testdb)
  (let ((env (mdb-env-create)))
    (mdb-env-open env "tests/testdb" 0
		  (bitwise-ior perm/irusr perm/iwusr perm/irgrp perm/iroth))
    (let* ((txn (mdb-txn-begin env #f 0))
	   (dbi (mdb-dbi-open txn #f 0)))
      (mdb-put txn dbi "foo" "bar" 0)
      (mdb-txn-commit txn))
    (let* ((txn (mdb-txn-begin env #f 0))
	   (dbi (mdb-dbi-open txn #f 0)))
      (test "bar" (mdb-get txn dbi "foo"))
      (mdb-txn-commit txn))
    (mdb-env-close env)))

(test-group "not found error condition"
  (clear-testdb)
  (let ((env (mdb-env-create)))
    (mdb-env-open env "tests/testdb" 0
		  (bitwise-ior perm/irusr perm/iwusr perm/irgrp perm/iroth))
    (let* ((txn (mdb-txn-begin env #f 0))
	   (dbi (mdb-dbi-open txn #f 0)))
      (test 'not-found
	    (condition-case (mdb-get txn dbi "foo")
	      ((exn lmdb MDB_NOTFOUND) 'not-found)))
      (mdb-txn-commit txn))
    (mdb-env-close env)))

(test-group "abort transaction"
  (clear-testdb)
  (let ((env (mdb-env-create)))
    (mdb-env-open env "tests/testdb" 0
		  (bitwise-ior perm/irusr perm/iwusr perm/irgrp perm/iroth))
    (let* ((txn (mdb-txn-begin env #f 0))
	   (dbi (mdb-dbi-open txn #f 0)))
      (mdb-put txn dbi "foo" "one" 0)
      (mdb-txn-commit txn))
    (let* ((txn (mdb-txn-begin env #f 0))
	   (dbi (mdb-dbi-open txn #f 0)))
      (mdb-put txn dbi "bar" "two" 0)
      (mdb-txn-abort txn))
    (let* ((txn (mdb-txn-begin env #f 0))
	   (dbi (mdb-dbi-open txn #f 0)))
      (test "one" (mdb-get txn dbi "foo"))
      (test 'not-found
	    (condition-case (mdb-get txn dbi "bar")
	      ((exn lmdb MDB_NOTFOUND) 'not-found)))
      (mdb-txn-commit txn))
    (mdb-env-close env)))

(test-group "copy environment to filename"
  (clear-testdb)
  (let ((env (mdb-env-create)))
    (mdb-env-open env "tests/testdb" 0
		  (bitwise-ior perm/irusr perm/iwusr perm/irgrp perm/iroth))
    (let* ((txn (mdb-txn-begin env #f 0))
	   (dbi (mdb-dbi-open txn #f 0)))
      (mdb-put txn dbi "foo" "one" 0)
      (mdb-put txn dbi "bar" "two" 0)
      (mdb-txn-commit txn))
    (when (file-exists? "tests/testdb2")
      (delete-directory "tests/testdb2" #t))
    (create-directory "tests/testdb2")
    (mdb-env-copy env "tests/testdb2")
    (mdb-env-close env))
  (let ((env (mdb-env-create)))
    (mdb-env-open env "tests/testdb2" 0
		  (bitwise-ior perm/irusr perm/iwusr perm/irgrp perm/iroth))
    (let* ((txn (mdb-txn-begin env #f 0))
	   (dbi (mdb-dbi-open txn #f 0)))
      (test "one" (mdb-get txn dbi "foo"))
      (test "two" (mdb-get txn dbi "bar"))
      (mdb-txn-commit txn))
    (mdb-env-close env)))

(test-exit)
