package peer

import (
	"fmt"

	"github.com/juju/errgo"
)

var (
	maskAny = errgo.MaskFunc(errgo.Any)
)

func maskAnyf(err error, f string, v ...interface{}) error {
	if err == nil {
		return nil
	}

	f = fmt.Sprintf("%s: %s", err.Error(), f)
	newErr := errgo.WithCausef(nil, errgo.Cause(err), f, v...)
	newErr.(*errgo.Err).SetLocation(1)

	return newErr
}

var alreadyExistsError = errgo.New("already exists")

// IsAlreadyExists asserts alreadyExistsError.
func IsAlreadyExists(err error) bool {
	return errgo.Cause(err) == alreadyExistsError
}

var deprecatedError = errgo.New("deprecated")

// IsDeprecated asserts deprecatedError.
func IsDeprecated(err error) bool {
	return errgo.Cause(err) == deprecatedError
}

var invalidConfigError = errgo.New("invalid config")

// IsInvalidConfig asserts invalidConfigError.
func IsInvalidConfig(err error) bool {
	return errgo.Cause(err) == invalidConfigError
}

var invalidPositionError = errgo.New("invalid position")

// IsInvalidPosition asserts invalidPositionError.
func IsInvalidPosition(err error) bool {
	return errgo.Cause(err) == invalidPositionError
}

var notFoundError = errgo.New("not found")

// IsNotFound asserts notFoundError.
func IsNotFound(err error) bool {
	return errgo.Cause(err) == notFoundError
}
