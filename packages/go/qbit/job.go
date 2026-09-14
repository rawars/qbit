package qbit

// Job is a unit of work stored by Qbit.
type Job struct {
	ID      string
	Name    string
	Group   string
	Payload []byte
	Token   string
	// Attempts is the number of times this job has been reserved, including the
	// current reservation.
	Attempts int
	// Duplicate reports that Add returned a previously-created job with the
	// same caller-provided ID.
	Duplicate bool
}
