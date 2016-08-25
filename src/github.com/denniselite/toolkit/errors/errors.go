package errors

import (
	"log"
	"encoding/json"
	"net/http"
	"gopkg.in/validator.v2"
	"os"
	"bufio"
	"strings"
	"runtime"
	"errors"
	"sort"
)

const (
	OperationIsNotAllowed = 7000

	DatabaseException = 8000
	RabbitException = 8010
)

const (
	DetectError = -1
	InternalError = 1000
	ValidationError = 1001
	BadRequest = 1002
	RequestTimeout = 1003

	InternalErrorMessage = "Internal error"
)

var errorMessages = map[int]string {
	InternalError: "Internal error",
	ValidationError: "Field errors",
	BadRequest: "Bad request",
	RequestTimeout: "Request timeout",
}

var errorStatuses = map[int]int {
	InternalError: http.StatusInternalServerError,
	ValidationError: http.StatusBadRequest,
	BadRequest: http.StatusBadRequest,
	RequestTimeout: http.StatusGatewayTimeout,
}

type HttpErrorResponse struct {
	Error       Error        `json:"error"`
}

type Error struct {
	Code        int          `json:"code"`
	Message     string       `json:"message"`
	FieldErrors FieldErrors  `json:"fieldErrors,omitempty"`
}

type FieldErrors []FieldError

type FieldError struct {
	Field       string       `json:"field"`
	Error       string       `json:"error"`
}

func NewError(err error, code int) Error {
	if err != nil {
		log.Println(err)
	}

	apiError := Error{Code: code}
	if code == DetectError {
		if errs, ok := err.(validator.ErrorMap); ok {
			apiError.Code = ValidationError
			for field, fieldError := range errs {
				apiError.FieldErrors = append(apiError.FieldErrors, FieldError{Field: field, Error: fieldError.Error()})
			}
			sort.Sort(apiError.FieldErrors)
		} else {
			apiError.Code = InternalError
		}
	}
	apiError.Message = errorMessages[apiError.Code]

	return apiError
}

func HttpApiError(err error) (status int, result HttpErrorResponse) {
	var ok bool
	var apiError Error

	if apiError, ok = err.(Error); !ok {
		apiError = NewError(err, DetectError)
	}

	status, ok = errorStatuses[apiError.Code]
	if !ok {
		status = http.StatusInternalServerError
	}

	result = HttpErrorResponse{Error: apiError}
	return
}

func (a Error) Error() string {
	return a.Message
}

func (a Error) GetCode() int {
	return a.Code
}

func (a Error) GetJson() []byte {
	res, _ := json.Marshal(a)
	return res
}

func (f FieldErrors) Len() int {
	return len(f)
}

func (f FieldErrors) Less(i, j int) bool {
	return f[i].Field < f[j].Field
}

func (f FieldErrors) Swap(i, j int) {
	f[i], f[j] = f[j], f[i]
}

func Oops(err error) {
	if err != nil {
		log.Println(err)
		log.Println(readErrorFileLines())
		panic(err)
	}
}

func readErrorFileLines() map[int]string {
	_, filePath, errorLine, _ := runtime.Caller(10)
	lines := make(map[int]string)

	file, err := os.Open(filePath)
	if err != nil {
		return lines
	}

	defer file.Close()

	reader := bufio.NewReader(file)
	currentLine := 0
	for {
		line, err := reader.ReadString('\n')
		if err != nil || currentLine > errorLine + 5 {
			break
		}

		currentLine++

		if currentLine >= errorLine - 5 {
			lines[currentLine] = strings.Replace(line, "\n", "", -1)
		}
	}

	return lines
}

func getMsg(code int) (s string) {
	switch code {
	case OperationIsNotAllowed :
		s = "Operation is not allowed"
	case DatabaseException :
		s = "Database Exception"
	case RabbitException :
		s = "Rabbit Exception"
	}
	return
}

func NewInternalError(err error) error {
	log.Println(err)
	return errors.New("Internal error")
}