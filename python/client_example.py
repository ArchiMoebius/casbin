#!/usr/bin/env python3
"""gRPC reflection client for the KV service.

This client uses gRPC server reflection to dynamically discover available
services and methods at runtime, similar to grpcurl. It supports both
Basic auth and JWT auth.

Usage:
    # List all services
    python grpc_client.py list

    # List methods in a service
    python grpc_client.py list kv.v1.KVService

    # Describe a method
    python grpc_client.py describe kv.v1.KVService.Get

    # Invoke Get with Basic auth (note: --auth comes BEFORE 'call')
    python grpc_client.py --auth basic --user alice:password123 call kv.v1.KVService.Get '{"key": "foo"}'

    # Invoke Put with JWT auth
    python grpc_client.py --auth jwt --token <jwt-token> call kv.v1.KVService.Put '{"key": "foo", "value": "bar"}'

    # Watch (streaming) with Basic auth
    python grpc_client.py --auth basic --user alice:password123 call kv.v1.KVService.Watch '{"key": "foo"}'
    
    # Connect to proxy instead of direct backend
    python grpc_client.py --server localhost:50052 list
"""

import argparse
import base64
import grpc
import json
import sys
import time
from typing import Optional, Dict, Any, List

from grpc_reflection.v1alpha import reflection_pb2, reflection_pb2_grpc
from google.protobuf import descriptor_pool, message_factory
from google.protobuf.json_format import Parse, MessageToDict
from google.protobuf.descriptor_pb2 import FileDescriptorProto, FieldDescriptorProto


# Map protobuf type numbers to human-readable names
FIELD_TYPE_NAMES = {
    FieldDescriptorProto.TYPE_DOUBLE: "double",
    FieldDescriptorProto.TYPE_FLOAT: "float",
    FieldDescriptorProto.TYPE_INT64: "int64",
    FieldDescriptorProto.TYPE_UINT64: "uint64",
    FieldDescriptorProto.TYPE_INT32: "int32",
    FieldDescriptorProto.TYPE_FIXED64: "fixed64",
    FieldDescriptorProto.TYPE_FIXED32: "fixed32",
    FieldDescriptorProto.TYPE_BOOL: "bool",
    FieldDescriptorProto.TYPE_STRING: "string",
    FieldDescriptorProto.TYPE_GROUP: "group",
    FieldDescriptorProto.TYPE_MESSAGE: "message",
    FieldDescriptorProto.TYPE_BYTES: "bytes",
    FieldDescriptorProto.TYPE_UINT32: "uint32",
    FieldDescriptorProto.TYPE_ENUM: "enum",
    FieldDescriptorProto.TYPE_SFIXED32: "sfixed32",
    FieldDescriptorProto.TYPE_SFIXED64: "sfixed64",
    FieldDescriptorProto.TYPE_SINT32: "sint32",
    FieldDescriptorProto.TYPE_SINT64: "sint64",
}


class GrpcReflectionClient:
    """Client that uses gRPC reflection to interact with services."""

    def __init__(self, address: str = "localhost:50051"):
        self.address = address
        self.channel = grpc.insecure_channel(address)
        self.reflection_stub = reflection_pb2_grpc.ServerReflectionStub(self.channel)
        self.pool = descriptor_pool.DescriptorPool()
        self._loaded_files = set()

    def close(self):
        """Close the gRPC channel."""
        self.channel.close()

    def _get_file_descriptor(self, filename: str) -> FileDescriptorProto:
        """Request a file descriptor from the server via reflection."""
        request = reflection_pb2.ServerReflectionRequest(
            file_by_filename=filename
        )
        responses = self.reflection_stub.ServerReflectionInfo(iter([request]))
        for response in responses:
            if response.HasField("file_descriptor_response"):
                for proto_bytes in response.file_descriptor_response.file_descriptor_proto:
                    fd_proto = FileDescriptorProto()
                    fd_proto.ParseFromString(proto_bytes)
                    return fd_proto
        raise ValueError(f"File descriptor for {filename} not found")

    def _get_file_containing_symbol(self, symbol: str) -> List[FileDescriptorProto]:
        """Request file descriptors containing a symbol from the server."""
        request = reflection_pb2.ServerReflectionRequest(
            file_containing_symbol=symbol
        )
        responses = self.reflection_stub.ServerReflectionInfo(iter([request]))
        file_descriptors = []
        for response in responses:
            if response.HasField("file_descriptor_response"):
                for proto_bytes in response.file_descriptor_response.file_descriptor_proto:
                    fd_proto = FileDescriptorProto()
                    fd_proto.ParseFromString(proto_bytes)
                    file_descriptors.append(fd_proto)
        return file_descriptors

    def _load_file_descriptor(self, filename: str):
        """Load a file descriptor and all its dependencies recursively."""
        if filename in self._loaded_files:
            return

        try:
            fd_proto = self._get_file_descriptor(filename)
        except Exception as e:
            # If we can't get the file by name, it might be a well-known type
            # that's already in the pool. Skip silently.
            return

        # Load dependencies first (depth-first)
        for dep in fd_proto.dependency:
            if dep not in self._loaded_files:
                try:
                    self._load_file_descriptor(dep)
                except Exception:
                    # Well-known types like google/protobuf/* may not be available
                    # via reflection but are already in the pool
                    pass

        # Add to pool if not already there
        try:
            self.pool.Add(fd_proto)
            self._loaded_files.add(fd_proto.name)
        except Exception:
            # Already added - this is fine
            pass

    def list_services(self) -> List[str]:
        """List all services exposed by the server."""
        request = reflection_pb2.ServerReflectionRequest(
            list_services=""
        )
        responses = self.reflection_stub.ServerReflectionInfo(iter([request]))
        services = []
        for response in responses:
            if response.HasField("list_services_response"):
                for service in response.list_services_response.service:
                    services.append(service.name)
        return services

    def _resolve_service(self, service_name: str):
        """Load the file descriptor for a service and all its dependencies."""
        file_descriptors = self._get_file_containing_symbol(service_name)
        
        if not file_descriptors:
            raise ValueError(f"Service {service_name} not found")

        # Load all returned file descriptors
        for fd_proto in file_descriptors:
            # Load dependencies first
            for dep in fd_proto.dependency:
                try:
                    self._load_file_descriptor(dep)
                except Exception:
                    pass

            # Add the file descriptor
            try:
                self.pool.Add(fd_proto)
                self._loaded_files.add(fd_proto.name)
            except Exception:
                # Already loaded
                pass

    def describe_service(self, service_name: str) -> Dict[str, Any]:
        """Get detailed information about a service and its methods."""
        self._resolve_service(service_name)
        service_desc = self.pool.FindServiceByName(service_name)

        methods = []
        for method in service_desc.methods:
            method_info = {
                "name": method.name,
                "input_type": method.input_type.full_name,
                "output_type": method.output_type.full_name,
                "client_streaming": method.client_streaming,
                "server_streaming": method.server_streaming,
                "options": {},
            }

            # Extract method options if they exist
            try:
                opts = method.GetOptions()
                if opts:
                    for field, value in opts.ListFields():
                        method_info["options"][field.full_name] = str(value)
            except Exception:
                # No options or error accessing them
                pass

            methods.append(method_info)

        return {
            "name": service_desc.full_name,
            "methods": methods,
        }

    def describe_message(self, message_name: str) -> Dict[str, Any]:
        """Get the schema for a message type."""
        # Try to find the message in the already-loaded pool
        try:
            msg_desc = self.pool.FindMessageTypeByName(message_name)
        except KeyError:
            # Message not loaded yet. Try to infer the service name and load it.
            # Message names like "kv.v1.GetRequest" -> try service "kv.v1.KVService"
            parts = message_name.rsplit(".", 1)
            if len(parts) == 2:
                package_name = parts[0]
                # Try common service naming patterns
                for service_suffix in ["Service", "API", ""]:
                    try:
                        potential_service = f"{package_name}.KV{service_suffix}"
                        self._resolve_service(potential_service)
                        msg_desc = self.pool.FindMessageTypeByName(message_name)
                        break
                    except Exception:
                        continue
                else:
                    raise ValueError(
                        f"Message type {message_name} not found. "
                        "Try describing the service first."
                    )
            else:
                raise ValueError(f"Invalid message name: {message_name}")

        fields = []
        for field in msg_desc.fields:
            type_name = None
            if field.type == FieldDescriptorProto.TYPE_MESSAGE:
                type_name = field.message_type.full_name if field.message_type else None
            elif field.type == FieldDescriptorProto.TYPE_ENUM:
                type_name = field.enum_type.full_name if field.enum_type else None

            fields.append({
                "name": field.name,
                "number": field.number,
                "type": field.type,
                "type_name": type_name,
            })

        return {
            "name": msg_desc.full_name,
            "fields": fields,
        }


    def invoke_rpc(
        self,
        method_path: str,
        request_json: str,
        auth_header: Optional[str] = None,
        timeout: Optional[float] = None,
    ):
        """
        Invoke an RPC method.

        Args:
            method_path: Full method path like "kv.v1.KVService.Get" or "kv.v1.KVService/Get"
            request_json: JSON string of the request message
            auth_header: Optional Authorization header value
            timeout: Optional timeout in seconds for the RPC

        Returns:
            For unary RPCs: dict with response
            For streaming RPCs: list of dicts with responses
        """
        # Parse method path - support both "." and "/" as separators
        if "/" in method_path:
            parts = method_path.rsplit("/", 1)
        else:
            parts = method_path.rsplit(".", 1)

        if len(parts) != 2:
            raise ValueError(
                "Method path must be in format Service.Method or Service/Method"
            )

        service_name = parts[0]
        method_name = parts[1]

        # Load service descriptor
        self._resolve_service(service_name)
        service_desc = self.pool.FindServiceByName(service_name)
        method_desc = service_desc.FindMethodByName(method_name)

        if not method_desc:
            raise ValueError(f"Method {method_name} not found in service {service_name}")

        # Get message descriptors
        input_desc = method_desc.input_type
        output_desc = method_desc.output_type

        # Create dynamic message classes using reflection
        from google.protobuf import reflection, message
        
        input_class = reflection.GeneratedProtocolMessageType(
            str(input_desc.name),
            (message.Message,),
            {'DESCRIPTOR': input_desc, '__module__': None}
        )
        output_class = reflection.GeneratedProtocolMessageType(
            str(output_desc.name),
            (message.Message,),
            {'DESCRIPTOR': output_desc, '__module__': None}
        )

        # Parse request JSON to protobuf message
        request = Parse(request_json, input_class())

        # Build metadata
        metadata = []
        if auth_header:
            metadata.append(("authorization", auth_header))

        # Create the gRPC method callable
        method_full_path = f"/{service_desc.full_name}/{method_desc.name}"

        # Determine RPC type and invoke accordingly
        if method_desc.client_streaming and method_desc.server_streaming:
            raise NotImplementedError("Bidirectional streaming not yet supported")
        elif method_desc.client_streaming:
            raise NotImplementedError("Client streaming not yet supported")
        elif method_desc.server_streaming:
            # Server streaming
            return self._invoke_streaming(
                method_full_path,
                request,
                output_class,
                metadata,
                timeout,
            )
        else:
            # Unary
            return self._invoke_unary(
                method_full_path,
                request,
                output_class,
                metadata,
                timeout,
            )

    def _invoke_unary(
        self,
        method_path: str,
        request,
        output_class,
        metadata,
        timeout: Optional[float],
    ):
        """Invoke a unary RPC."""
        response = self.channel.unary_unary(
            method_path,
            request_serializer=lambda x: x.SerializeToString(),
            response_deserializer=output_class.FromString,
        )(request, metadata=metadata, timeout=timeout)

        return MessageToDict(
            response,
            preserving_proto_field_name=True,
        )

    def _invoke_streaming(
        self,
        method_path: str,
        request,
        output_class,
        metadata,
        timeout: Optional[float],
    ):
        """Invoke a server-streaming RPC."""
        responses = self.channel.unary_stream(
            method_path,
            request_serializer=lambda x: x.SerializeToString(),
            response_deserializer=output_class.FromString,
        )(request, metadata=metadata, timeout=timeout)

        results = []
        try:
            for response in responses:
                result = MessageToDict(
                    response,
                    preserving_proto_field_name=True,
                )
                print(f"📦 Received: {json.dumps(result, indent=2)}")
                results.append(result)
        except grpc.RpcError as e:
            # Check if this is a normal stream termination
            if e.code() == grpc.StatusCode.CANCELLED:
                print(f"⏱️  Stream cancelled (timeout or client closed)")
            elif e.code() == grpc.StatusCode.DEADLINE_EXCEEDED:
                print(f"⏱️  Stream deadline exceeded")
            else:
                print(f"❌ Stream ended: {e.code()} - {e.details()}")

        return results


def build_auth_header(args) -> Optional[str]:
    """Build the Authorization header from CLI args."""
    if args.auth == "basic":
        if not args.user:
            print("❌ --user required for Basic auth (format: username:password)")
            sys.exit(1)
        encoded = base64.b64encode(args.user.encode()).decode()
        return f"Basic {encoded}"
    elif args.auth == "jwt":
        if not args.token:
            print("❌ --token required for JWT auth")
            sys.exit(1)
        return f"Bearer {args.token}"
    return None


def parse_method_path(target: str) -> tuple[str, Optional[str]]:
    """
    Parse a target string into (service_name, method_name).
    
    Examples:
        "kv.v1.KVService" -> ("kv.v1.KVService", None)
        "kv.v1.KVService.Get" -> ("kv.v1.KVService", "Get")
        "kv.v1.GetRequest" -> ("kv.v1.GetRequest", None)  # message type
    
    Returns:
        (service_name, method_name) where method_name is None if not specified
    """
    parts = target.split(".")
    
    # If we have at least 3 parts and the last part looks like a method name
    if len(parts) >= 3:
        last_part = parts[-1]
        # Methods typically start with uppercase: Get, Put, Watch
        # If it doesn't look like a protobuf message type suffix, assume it's a method
        if last_part[0].isupper() and not any(
            last_part.endswith(suffix) 
            for suffix in ["Request", "Response", "Service", "API", "Proto"]
        ):
            # This looks like a method name
            service_name = ".".join(parts[:-1])
            method_name = last_part
            return (service_name, method_name)
    
    # Otherwise, it's just a service or message name
    return (target, None)


def main():
    parser = argparse.ArgumentParser(
        description="gRPC reflection client for KV service",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    
    # Global options that apply to all subcommands
    parser.add_argument(
        "--server",
        default="localhost:50051",
        help="Server address (default: localhost:50051)",
    )
    parser.add_argument(
        "--auth",
        choices=["basic", "jwt", "none"],
        default="none",
        help="Authentication method",
    )
    parser.add_argument(
        "--user",
        help="Username:password for Basic auth",
    )
    parser.add_argument(
        "--token",
        help="JWT token for JWT auth",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        help="RPC timeout in seconds (for streaming calls)",
    )

    subparsers = parser.add_subparsers(dest="command", help="Command to run")

    # List command
    list_parser = subparsers.add_parser("list", help="List services or methods")
    list_parser.add_argument(
        "service",
        nargs="?",
        help="Service name to list methods (omit to list all services)",
    )

    # Describe command
    describe_parser = subparsers.add_parser(
        "describe", help="Describe a service, method, or message"
    )
    describe_parser.add_argument(
        "target",
        help="Service, method, or message name (e.g., kv.v1.KVService.Get)",
    )

    # Call command
    call_parser = subparsers.add_parser("call", help="Invoke an RPC method")
    call_parser.add_argument(
        "method",
        help="Method path (e.g., kv.v1.KVService.Get or kv.v1.KVService/Get)",
    )
    call_parser.add_argument("request", help="Request JSON")

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    client = GrpcReflectionClient(args.server)

    try:
        if args.command == "list":
            if args.service:
                # List methods in a service
                info = client.describe_service(args.service)
                print(f"📋 Service: {info['name']}")
                print("\nMethods:")
                for method in info["methods"]:
                    streaming = ""
                    if method["client_streaming"]:
                        streaming += "client-streaming "
                    if method["server_streaming"]:
                        streaming += "server-streaming"
                    streaming = f" ({streaming.strip()})" if streaming else ""

                    print(f"  • {method['name']}{streaming}")
                    print(f"    Input:  {method['input_type']}")
                    print(f"    Output: {method['output_type']}")

                    if method.get("options"):
                        for opt_name, opt_value in method["options"].items():
                            short_name = opt_name.split(".")[-1]
                            print(f"    Option: {short_name} = {opt_value}")
                    print()
            else:
                # List all services
                services = client.list_services()
                print("📋 Available services:")
                for service in services:
                    print(f"  • {service}")

        elif args.command == "describe":
            # Parse the target to determine if it's a service, method, or message
            service_name, method_name = parse_method_path(args.target)
            
            try:
                # Try to describe as a service first
                info = client.describe_service(service_name)
                
                if method_name:
                    # Describe specific method
                    method_info = None
                    for m in info["methods"]:
                        if m["name"] == method_name:
                            method_info = m
                            break

                    if method_info:
                        print(f"📋 Method: {service_name}.{method_name}\n")
                        
                        streaming = []
                        if method_info["client_streaming"]:
                            streaming.append("client-streaming")
                        if method_info["server_streaming"]:
                            streaming.append("server-streaming")
                        if streaming:
                            print(f"Type: {', '.join(streaming)}\n")

                        print(f"Input:  {method_info['input_type']}")
                        print(f"Output: {method_info['output_type']}")

                        if method_info.get("options"):
                            print("\nOptions:")
                            for opt_name, opt_value in method_info["options"].items():
                                short_name = opt_name.split(".")[-1]
                                print(f"  • {short_name}: {opt_value}")

                        print()

                        # Show message schemas
                        try:
                            print("Input message schema:")
                            input_msg = client.describe_message(
                                method_info["input_type"]
                            )
                            for field in input_msg["fields"]:
                                type_str = field.get("type_name") or FIELD_TYPE_NAMES.get(
                                    field["type"], f"type_{field['type']}"
                                )
                                print(f"  • {field['name']}: {type_str}")

                            print("\nOutput message schema:")
                            output_msg = client.describe_message(
                                method_info["output_type"]
                            )
                            for field in output_msg["fields"]:
                                type_str = field.get("type_name") or FIELD_TYPE_NAMES.get(
                                    field["type"], f"type_{field['type']}"
                                )
                                print(f"  • {field['name']}: {type_str}")
                        except Exception as e:
                            print(f"\n⚠️  Could not load message schemas: {e}")
                    else:
                        print(f"❌ Method {method_name} not found in {service_name}")
                        sys.exit(1)
                else:
                    # Describe entire service
                    print(f"📋 Service: {info['name']}\n")
                    print("Methods:")
                    for method in info["methods"]:
                        streaming = ""
                        if method["client_streaming"]:
                            streaming += "client-streaming "
                        if method["server_streaming"]:
                            streaming += "server-streaming"
                        streaming = f" ({streaming.strip()})" if streaming else ""

                        print(f"  • {method['name']}{streaming}")
                        print(f"    Input:  {method['input_type']}")
                        print(f"    Output: {method['output_type']}")

                        if method.get("options"):
                            for opt_name, opt_value in method["options"].items():
                                short_name = opt_name.split(".")[-1]
                                print(f"    Option: {short_name} = {opt_value}")
                        print()

            except Exception as service_error:
                # Not a service - try as message type
                try:
                    info = client.describe_message(args.target)
                    print(f"📋 Message: {info['name']}\n")
                    print("Fields:")
                    for field in info["fields"]:
                        type_str = field.get("type_name") or FIELD_TYPE_NAMES.get(
                            field["type"], f"type_{field['type']}"
                        )
                        print(f"  • {field['name']}: {type_str}")
                except Exception as msg_error:
                    print(f"❌ Could not find service, method, or message: {args.target}")
                    print(f"\nService error: {service_error}")
                    print(f"Message error: {msg_error}")
                    print(f"\nTip: Use one of these formats:")
                    print(f"  • Service: kv.v1.KVService")
                    print(f"  • Method: kv.v1.KVService.Get")
                    print(f"  • Message: kv.v1.GetRequest")
                    sys.exit(1)

        elif args.command == "call":
            auth_header = build_auth_header(args)

            print(f"🚀 Invoking {args.method}...")
            print(f"📤 Request: {args.request}")
            if auth_header:
                auth_type = "Basic" if args.auth == "basic" else "JWT"
                print(f"🔐 Auth: {auth_type}")
            if args.timeout:
                print(f"⏱️  Timeout: {args.timeout}s")
            print()

            result = client.invoke_rpc(
                args.method,
                args.request,
                auth_header,
                timeout=args.timeout,
            )

            if isinstance(result, list):
                print(f"\n✅ Received {len(result)} streaming responses")
            else:
                print(f"📥 Response: {json.dumps(result, indent=2)}")

    except grpc.RpcError as e:
        print(f"❌ RPC Error: {e.code()}")
        print(f"   Details: {e.details()}")
        sys.exit(1)
    except KeyboardInterrupt:
        print("\n⚠️  Interrupted by user")
        sys.exit(0)
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        client.close()


if __name__ == "__main__":
    main()