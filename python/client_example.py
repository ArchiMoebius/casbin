#!/usr/bin/env python3
"""
gRPC reflection client for the KV service.

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

    # Invoke Get with Basic auth
    python grpc_client.py call kv.v1.KVService.Get '{"key": "foo"}' --auth basic --user alice:password123

    # Invoke Put with JWT auth
    python grpc_client.py call kv.v1.KVService.Put '{"key": "foo", "value": "bar"}' --auth jwt --token <jwt-token>

    # Watch (streaming) with Basic auth
    python grpc_client.py call kv.v1.KVService.Watch '{"key": "foo"}' --auth basic --user alice:password123
"""

import argparse
import base64
import grpc
import json
import sys
import time
from typing import Optional, Dict, Any

from grpc_reflection.v1alpha import reflection_pb2, reflection_pb2_grpc
from google.protobuf import descriptor_pool, symbol_database
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
        self.symbol_db = symbol_database.SymbolDatabase(pool=self.pool)
        self._loaded_files = set()

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

    def _load_file_descriptor(self, filename: str):
        """Load a file descriptor and all its dependencies."""
        if filename in self._loaded_files:
            return

        fd_proto = self._get_file_descriptor(filename)

        # Load dependencies first
        for dep in fd_proto.dependency:
            if dep not in self._loaded_files:
                try:
                    self._load_file_descriptor(dep)
                except Exception as e:
                    # Some dependencies like google/protobuf/* may already be loaded
                    pass

        # Add to pool if not already there
        try:
            self.pool.Add(fd_proto)
            self._loaded_files.add(filename)
        except Exception:
            # Already added
            pass

    def list_services(self) -> list[str]:
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
        """Load the file descriptor for a service."""
        request = reflection_pb2.ServerReflectionRequest(
            file_containing_symbol=service_name
        )
        responses = self.reflection_stub.ServerReflectionInfo(iter([request]))
        for response in responses:
            if response.HasField("file_descriptor_response"):
                for proto_bytes in response.file_descriptor_response.file_descriptor_proto:
                    fd_proto = FileDescriptorProto()
                    fd_proto.ParseFromString(proto_bytes)
                    # Load this file and its dependencies
                    for dep in fd_proto.dependency:
                        try:
                            self._load_file_descriptor(dep)
                        except Exception:
                            pass
                    self.pool.Add(fd_proto)
                    self._loaded_files.add(fd_proto.name)
                    return

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
            if method.options:
                # The options are a protobuf message, we can inspect its fields
                # Custom extensions are stored in the Extensions field
                from google.protobuf import descriptor_pb2
                opts = method.options
                
                # Try to extract all set fields from the options
                for field, value in opts.ListFields():
                    # field.full_name gives us the option name
                    # For custom options, it will be like "options.v1.required_action"
                    method_info["options"][field.full_name] = str(value)
            
            methods.append(method_info)

        return {
            "name": service_desc.full_name,
            "methods": methods,
        }

    def describe_message(self, message_name: str) -> Dict[str, Any]:
        """Get the schema for a message type."""
        # The message descriptor might not be loaded yet. Try to infer which
        # service file it came from and load that first.
        # Message names are like "kv.v1.GetRequest", services are "kv.v1.KVService"
        parts = message_name.rsplit(".", 1)
        if len(parts) == 2:
            # Try to load the service in the same package
            package_name = parts[0]
            # Common pattern: messages are in the same file as their service
            # Try a few common service name patterns
            for service_suffix in ["Service", "API", ""]:
                try:
                    potential_service = f"{package_name}.KV{service_suffix}"
                    self._resolve_service(potential_service)
                    break
                except Exception:
                    pass

        try:
            msg_desc = self.pool.FindMessageTypeByName(message_name)
        except KeyError:
            # If still not found, try a broader search by loading all descriptors
            # that might contain this message
            raise ValueError(f"Message type {message_name} not found. Try describing the service first.")

        fields = []
        for field in msg_desc.fields:
            # type_name only exists for message/enum types, not primitives
            type_name = None
            if hasattr(field, 'type_name') and field.type_name:
                type_name = field.type_name
            
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
    ):
        """
        Invoke an RPC method.

        Args:
            method_path: Full method path like "kv.v1.KVService/Get"
            request_json: JSON string of the request message
            auth_header: Optional Authorization header value
        """
        # Parse method path
        parts = method_path.rsplit(".", 1)
        if len(parts) != 2:
            parts = method_path.rsplit("/", 1)
            if len(parts) != 2:
                raise ValueError("Method path must be in format Service.Method or Service/Method")

        service_name = parts[0]
        method_name = parts[1]

        # Load service descriptor
        self._resolve_service(service_name)
        service_desc = self.pool.FindServiceByName(service_name)
        method_desc = service_desc.FindMethodByName(method_name)

        if not method_desc:
            raise ValueError(f"Method {method_name} not found in service {service_name}")

        # Get message types
        input_type = self.symbol_db.GetPrototype(method_desc.input_type)
        output_type = self.symbol_db.GetPrototype(method_desc.output_type)

        # Parse request
        request = Parse(request_json, input_type())

        # Build metadata
        metadata = []
        if auth_header:
            metadata.append(("authorization", auth_header))

        # Create the gRPC method callable
        method_full_path = f"/{service_desc.full_name}/{method_desc.name}"

        if method_desc.server_streaming:
            # Streaming RPC
            return self._invoke_streaming(
                method_full_path,
                request,
                output_type,
                metadata,
            )
        else:
            # Unary RPC
            return self._invoke_unary(
                method_full_path,
                request,
                output_type,
                metadata,
            )

    def _invoke_unary(self, method_path, request, output_type, metadata):
        """Invoke a unary RPC."""
        response = self.channel.unary_unary(
            method_path,
            request_serializer=lambda x: x.SerializeToString(),
            response_deserializer=output_type.FromString,
        )(request, metadata=metadata)

        return MessageToDict(response, preserving_proto_field_name=True)

    def _invoke_streaming(self, method_path, request, output_type, metadata):
        """Invoke a server-streaming RPC."""
        responses = self.channel.unary_stream(
            method_path,
            request_serializer=lambda x: x.SerializeToString(),
            response_deserializer=output_type.FromString,
        )(request, metadata=metadata)

        results = []
        try:
            for response in responses:
                result = MessageToDict(response, preserving_proto_field_name=True)
                print(f"📦 Received: {json.dumps(result, indent=2)}")
                results.append(result)
        except grpc.RpcError as e:
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


def main():
    parser = argparse.ArgumentParser(
        description="gRPC reflection client for KV service",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
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

    subparsers = parser.add_subparsers(dest="command", help="Command to run")

    # List command
    list_parser = subparsers.add_parser("list", help="List services or methods")
    list_parser.add_argument(
        "service",
        nargs="?",
        help="Service name to list methods (omit to list all services)",
    )

    # Describe command
    describe_parser = subparsers.add_parser("describe", help="Describe a service or message")
    describe_parser.add_argument("target", help="Service or message name")

    # Call command
    call_parser = subparsers.add_parser("call", help="Invoke an RPC method")
    call_parser.add_argument("method", help="Method path (e.g., kv.v1.KVService.Get)")
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
                    print()
            else:
                # List all services
                services = client.list_services()
                print("📋 Available services:")
                for service in services:
                    print(f"  • {service}")

        elif args.command == "describe":
            # Check if this is a method path (Service.Method or Service/Method)
            if "." in args.target and args.target.count(".") >= 2:
                # Might be Service.Method — try to parse it
                parts = args.target.rsplit(".", 1)
                service_name = parts[0]
                method_name = parts[1] if len(parts) == 2 else None

                try:
                    # First, load the service descriptor
                    info = client.describe_service(service_name)
                    
                    # Find the specific method
                    if method_name:
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
                            
                            # Show method options if any
                            if method_info.get("options"):
                                print("\nOptions:")
                                for opt_name, opt_value in method_info["options"].items():
                                    # Clean up the display - show just the option name and value
                                    # Convert "options.v1.required_action" to "required_action"
                                    short_name = opt_name.split(".")[-1]
                                    print(f"  • {short_name}: {opt_value}")
                            
                            print()

                            # Show the input message schema
                            # The service descriptor is already loaded, so the message types are available
                            try:
                                print("Input message schema:")
                                input_msg = client.describe_message(method_info['input_type'])
                                for field in input_msg["fields"]:
                                    type_str = field.get("type_name") or FIELD_TYPE_NAMES.get(field['type'], f"type_{field['type']}")
                                    print(f"  • {field['name']}: {type_str}")

                                print("\nOutput message schema:")
                                output_msg = client.describe_message(method_info['output_type'])
                                for field in output_msg["fields"]:
                                    type_str = field.get("type_name") or FIELD_TYPE_NAMES.get(field['type'], f"type_{field['type']}")
                                    print(f"  • {field['name']}: {type_str}")
                            except Exception as e:
                                print(f"\n⚠️  Could not load message schemas: {e}")
                        else:
                            print(f"❌ Method {method_name} not found in service {service_name}")
                            sys.exit(1)
                    else:
                        # Just describe the service
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
                    # Not a service.method, try as a plain message type
                    try:
                        info = client.describe_message(args.target)
                        print(f"📋 Message: {info['name']}\n")
                        print("Fields:")
                        for field in info["fields"]:
                            type_str = field.get("type_name") or FIELD_TYPE_NAMES.get(field['type'], f"type_{field['type']}")
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
            else:
                # Simpler path - try service then message
                try:
                    info = client.describe_service(args.target)
                    print(f"📋 Service: {info['name']}\n")
                    print("Methods:")
                    for method in info["methods"]:
                        print(f"  • {method['name']}")
                        print(f"    Input:  {method['input_type']}")
                        print(f"    Output: {method['output_type']}")
                        
                        if method.get("options"):
                            for opt_name, opt_value in method["options"].items():
                                short_name = opt_name.split(".")[-1]
                                print(f"    Option: {short_name} = {opt_value}")
                        print()
                except Exception:
                    try:
                        info = client.describe_message(args.target)
                        print(f"📋 Message: {info['name']}\n")
                        print("Fields:")
                        for field in info["fields"]:
                            type_str = field.get("type_name") or FIELD_TYPE_NAMES.get(field['type'], f"type_{field['type']}")
                            print(f"  • {field['name']}: {type_str}")
                    except Exception as e:
                        print(f"❌ Could not find service or message: {args.target}")
                        print(f"Error: {e}")
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
            print()

            result = client.invoke_rpc(args.method, args.request, auth_header)

            if isinstance(result, list):
                print(f"\n✅ Received {len(result)} streaming responses")
            else:
                print(f"📥 Response: {json.dumps(result, indent=2)}")

    except grpc.RpcError as e:
        print(f"❌ RPC Error: {e.code()}")
        print(f"   Details: {e.details()}")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()