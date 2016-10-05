(module lmdb-lolevel

;; exports
(mdb-env-open
 mdb-env-close
 lmdb-env-pointer
 lmdb-dbi-pointer
 lmdb-txn-pointer)

(import chicken scheme foreign)
(use srfi-69)

(foreign-declare "#include <lmdb.h>")
(foreign-declare "#include <errno.h>")


;; This macro creates a variable associated with each foreign integer
;; and a hash-map which maps the integer back to a symbol - useful for
;; constructing error conditions.

(define-syntax lmdb-defs
  (syntax-rules ()
    ((_ mapname name ...)
     (begin
       (define name (foreign-value name int)) ...
       (define mapname
	 (alist->hash-table (list (cons name (quote name)) ...)
			    hash: number-hash
			    test: fx=))))))

;; Environment Flags
(lmdb-defs environment-flags
 MDB_FIXEDMAP
 MDB_NOSUBDIR
 MDB_NOSYNC
 MDB_RDONLY
 MDB_NOMETASYNC
 MDB_WRITEMAP
 MDB_MAPASYNC
 MDB_NOTLS
 MDB_NOLOCK
 MDB_NORDAHEAD
 MDB_NOMEMINIT)

;; Return Codes
(lmdb-defs return-codes
 MDB_SUCCESS
 MDB_KEYEXIST
 MDB_NOTFOUND
 MDB_PAGE_NOTFOUND
 MDB_CORRUPTED
 MDB_PANIC
 MDB_VERSION_MISMATCH
 MDB_INVALID
 MDB_MAP_FULL
 MDB_DBS_FULL
 MDB_READERS_FULL
 MDB_TLS_FULL
 MDB_TXN_FULL
 MDB_CURSOR_FULL
 MDB_PAGE_FULL
 MDB_MAP_RESIZED
 MDB_INCOMPATIBLE
 MDB_BAD_RSLOT
 MDB_BAD_TXN
 MDB_BAD_VALSIZE
 MDB_BAD_DBI
 
 ;; The following are not LMDB specific (defined in errno.h), but are
 ;; added to the hash table anyway as they can be helpful in
 ;; conditions e.g. (exn lmdb ENOENT). These integer codes can still
 ;; be passed to mdb_strerror as it's a superset of strerror(3).
 
 ENOENT
 EACCES
 EAGAIN)


;; Wrapping these LMDB pointers in records is not strictly necessary
;; but provides nicer REPL output and a little type protection.

(define-record lmdb-env pointer)
(define-record-printer (lmdb-env x port)
  (fprintf port "#<lmdb-env ~S>" (lmdb-env-pointer x)))

(define-record lmdb-dbi pointer)
(define-record-printer (lmdb-dbi x port)
  (fprintf port "#<lmdb-dbi ~S>" (lmdb-dbi-pointer x)))

(define-record lmdb-txn pointer)
(define-record-printer (lmdb-txn x port)
  (fprintf port "#<lmdb-txn ~S>" (lmdb-txn-pointer x)))


;; Wraps calls to the C API which may return an error code. If
;; anything other than MDB_SUCCESS is returned, a scheme (abort)
;; occurs with an appropriate lmdb condition.

(define-syntax check-return
  (syntax-rules ()
    ((_ location (name args ...))
     (let ((code (name args ...)))
       (when (not (fx= code MDB_SUCCESS))
	 (abort (lmdb-condition location code)))))))

(define (lmdb-condition name code)
  (make-composite-condition
   (make-property-condition 'exn
			    'message (mdb-strerror code)
			    'location name)
   (make-property-condition 'lmdb)
   (make-property-condition (hash-table-ref return-codes code))))


(define mdb-strerror
  (foreign-lambda c-string "mdb_strerror" int))

(define c-mdb_env_create
  (foreign-lambda int "mdb_env_create"
    (c-pointer (c-pointer (struct MDB_env)))))

(define (mdb-env-create)
  (let-location ((p c-pointer))
    (check-return 'mdb-env-create (c-mdb_env_create (location p)))
    (make-lmdb-env p)))

(define c-mdb_env_open
  (foreign-lambda int "mdb_env_open"
    (c-pointer (struct MDB_env))
    (const c-string)
    unsigned-int
    int))

(define (mdb-env-open path flags mode)
  (let ((env (mdb-env-create)))
    (check-return 'mdb-env-open
		  (c-mdb_env_open
		   (lmdb-env-pointer env)
		   path
		   flags
		   mode))
    env))

(define c-mdb_env_close
  (foreign-lambda void "mdb_env_close"
    (c-pointer (struct MDB_env))))

(define (mdb-env-close env)
  (check-return 'mdb-env-close (c-mdb_env_close (lmdb-env-pointer env)))
  (lmdb-env-pointer-set! env #f))

)
