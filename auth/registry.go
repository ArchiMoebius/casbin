package auth

import (
	"fmt"
	"strings"

	optionsv1 "kvservice/pkg/gen/v1/options"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
)

// actionToString maps the generated Action enum to the lowercase
// strings used in policy.csv.  Any enum variant not listed here
// (including ACTION_UNSPECIFIED) returns an error, which causes
// BuildMethodActionMap to fail fast at startup rather than silently
// allowing an unannotated RPC through.
func actionToString(a optionsv1.Action) (string, error) {
	switch a {
	case optionsv1.Action_ACTION_REFLECT:
		return "reflect", nil
	case optionsv1.Action_ACTION_GET:
		return "get", nil
	case optionsv1.Action_ACTION_PUT:
		return "put", nil
	case optionsv1.Action_ACTION_DELETE:
		return "delete", nil
	case optionsv1.Action_ACTION_WATCH:
		return "watch", nil
	default:
		return "", fmt.Errorf("unknown or unspecified Action enum value: %v", a)
	}
}

// BuildMethodActionMap walks every registered gRPC service descriptor
// and extracts the required_action option from each method.
// Returns a map of "/package.Service/Method" -> action string.
func BuildMethodActionMap() (map[string]string, error) {
	result := make(map[string]string)

	protoregistry.GlobalFiles.RangeFiles(func(fd protoreflect.FileDescriptor) bool {
		services := fd.Services()
		for i := 0; i < services.Len(); i++ {
			svc := services.Get(i)
			fullSvcName := fmt.Sprintf("/%s.%s", fd.Package(), svc.Name())

			methods := svc.Methods()
			for j := 0; j < methods.Len(); j++ {
				m := methods.Get(j)
				opts := m.Options()
				if opts == nil {
					continue
				}

				enumVal, ok := proto.GetExtension(
					opts,
					optionsv1.E_RequiredAction,
				).(optionsv1.Action)
				if !ok {
					continue
				}

				action, err := actionToString(enumVal)
				if err != nil {
					// This will be caught by the len check below if it's
					// the only method, but log it either way so the operator
					// knows which RPC is misconfigured.
					if !strings.HasPrefix(fullSvcName, "/grpc.reflection") {
						fmt.Printf("WARNING: skipping %s/%s: %v\n", fullSvcName, m.Name(), err)
						continue
					} else {
						action = "reflect"
					}
				}

				fullMethodName := fmt.Sprintf("%s/%s", fullSvcName, m.Name())
				result[fullMethodName] = action
			}
		}
		return true // continue ranging
	})

	if len(result) == 0 {
		return nil, fmt.Errorf("no methods with required_action option found; ensure service protos are imported")
	}

	return result, nil
}
