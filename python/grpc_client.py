#!/usr/bin/env python3
"""
gRPC reflection client — CLI + interactive REPL.

Usage:
    # List services
    python grpc_client.py list

    # List methods
    python grpc_client.py list kv.v1.KVService

    # Describe a method
    python grpc_client.py describe kv.v1.KVService.Get

    # Call with Basic auth
    python grpc_client.py --auth basic --user alice:password123 call \\
        kv.v1.KVService.Get '{"key": "foo"}'

    # Interactive REPL
    python grpc_client.py --auth basic --user alice:password123 repl

    # REPL with pre-seeded service context
    python grpc_client.py repl --service kv.v1.KVService
"""
from __future__ import annotations

import argparse
import base64
import json
import sys
import time
from typing import Any, Dict, Iterator, List, Optional

import grpc
from grpc_reflection.v1alpha import reflection_pb2, reflection_pb2_grpc
from google.protobuf import descriptor_pool as _dp
from google.protobuf import message_factory
from google.protobuf.descriptor_pb2 import FieldDescriptorProto, FileDescriptorProto
from google.protobuf.json_format import MessageToDict, Parse

FIELD_TYPE_NAMES = {
    FieldDescriptorProto.TYPE_DOUBLE:   "double",
    FieldDescriptorProto.TYPE_FLOAT:    "float",
    FieldDescriptorProto.TYPE_INT64:    "int64",
    FieldDescriptorProto.TYPE_UINT64:   "uint64",
    FieldDescriptorProto.TYPE_INT32:    "int32",
    FieldDescriptorProto.TYPE_FIXED64:  "fixed64",
    FieldDescriptorProto.TYPE_FIXED32:  "fixed32",
    FieldDescriptorProto.TYPE_BOOL:     "bool",
    FieldDescriptorProto.TYPE_STRING:   "string",
    FieldDescriptorProto.TYPE_GROUP:    "group",
    FieldDescriptorProto.TYPE_MESSAGE:  "message",
    FieldDescriptorProto.TYPE_BYTES:    "bytes",
    FieldDescriptorProto.TYPE_UINT32:   "uint32",
    FieldDescriptorProto.TYPE_ENUM:     "enum",
    FieldDescriptorProto.TYPE_SFIXED32: "sfixed32",
    FieldDescriptorProto.TYPE_SFIXED64: "sfixed64",
    FieldDescriptorProto.TYPE_SINT32:   "sint32",
    FieldDescriptorProto.TYPE_SINT64:   "sint64",
}


class GrpcReflectionClient:
    """Client that uses gRPC reflection to interact with services."""

    def __init__(self, address: str = "localhost:50051") -> None:
        self.address         = address
        self.channel         = grpc.insecure_channel(address)
        self.reflection_stub = reflection_pb2_grpc.ServerReflectionStub(self.channel)
        self.pool            = _dp.DescriptorPool()
        self._loaded_files: set[str] = set()

    def close(self) -> None:
        self.channel.close()

    # ──────────────────────────────────────────────────────────────────────
    # Reflection helpers
    # ──────────────────────────────────────────────────────────────────────

    def _get_file_descriptor(self, filename: str) -> FileDescriptorProto:
        request   = reflection_pb2.ServerReflectionRequest(file_by_filename=filename)
        responses = self.reflection_stub.ServerReflectionInfo(iter([request]))
        for response in responses:
            if response.HasField("file_descriptor_response"):
                for proto_bytes in response.file_descriptor_response.file_descriptor_proto:
                    fd = FileDescriptorProto()
                    fd.ParseFromString(proto_bytes)
                    return fd
        raise ValueError(f"File descriptor for {filename} not found")

    def _get_file_containing_symbol(self, symbol: str) -> List[FileDescriptorProto]:
        request   = reflection_pb2.ServerReflectionRequest(file_containing_symbol=symbol)
        responses = self.reflection_stub.ServerReflectionInfo(iter([request]))
        fds: List[FileDescriptorProto] = []
        for response in responses:
            if response.HasField("file_descriptor_response"):
                for proto_bytes in response.file_descriptor_response.file_descriptor_proto:
                    fd = FileDescriptorProto()
                    fd.ParseFromString(proto_bytes)
                    fds.append(fd)
        return fds

    def _load_file_descriptor(self, filename: str) -> None:
        if filename in self._loaded_files:
            return
        try:
            fd_proto = self._get_file_descriptor(filename)
        except Exception:
            return

        for dep in fd_proto.dependency:
            if dep not in self._loaded_files:
                self._load_file_descriptor(dep)

        try:
            self.pool.Add(fd_proto)
            self._loaded_files.add(fd_proto.name)
        except Exception:
            self._loaded_files.add(fd_proto.name)

        # Also register with the global pool so GetOptions() sees extensions
        try:
            _dp.Default().Add(fd_proto)
        except Exception:
            pass

    def _resolve_service(self, service_name: str) -> None:
        fds = self._get_file_containing_symbol(service_name)
        if not fds:
            raise ValueError(f"Service {service_name} not found")
        for fd_proto in fds:
            for dep in fd_proto.dependency:
                self._load_file_descriptor(dep)
            try:
                self.pool.Add(fd_proto)
                self._loaded_files.add(fd_proto.name)
            except Exception:
                self._loaded_files.add(fd_proto.name)
            try:
                _dp.Default().Add(fd_proto)
            except Exception:
                pass

    # ──────────────────────────────────────────────────────────────────────
    # Public API
    # ──────────────────────────────────────────────────────────────────────

    def list_services(self) -> List[str]:
        request   = reflection_pb2.ServerReflectionRequest(list_services="")
        responses = self.reflection_stub.ServerReflectionInfo(iter([request]))
        services: List[str] = []
        for response in responses:
            if response.HasField("list_services_response"):
                for svc in response.list_services_response.service:
                    services.append(svc.name)
        return services

    def describe_service(self, service_name: str) -> Dict[str, Any]:
        self._resolve_service(service_name)
        svc_desc = self.pool.FindServiceByName(service_name)
        methods  = []
        for method in svc_desc.methods:
            info: Dict[str, Any] = {
                "name":             method.name,
                "input_type":       method.input_type.full_name,
                "output_type":      method.output_type.full_name,
                "client_streaming": method.client_streaming,
                "server_streaming": method.server_streaming,
                "options":          {},
            }
            try:
                opts = method.GetOptions()
                if opts:
                    for field, value in opts.ListFields():
                        info["options"][field.full_name] = str(value)
            except Exception:
                pass
            methods.append(info)
        return {"name": svc_desc.full_name, "methods": methods}

    def describe_message(self, message_name: str) -> Dict[str, Any]:
        try:
            msg_desc = self.pool.FindMessageTypeByName(message_name)
        except KeyError:
            parts = message_name.rsplit(".", 1)
            if len(parts) == 2:
                self._resolve_service(parts[0] + ".KVService")
            msg_desc = self.pool.FindMessageTypeByName(message_name)

        fields = []
        for f in msg_desc.fields:
            type_name = None
            if f.type == FieldDescriptorProto.TYPE_MESSAGE:
                type_name = f.message_type.full_name if f.message_type else None
            elif f.type == FieldDescriptorProto.TYPE_ENUM:
                type_name = f.enum_type.full_name if f.enum_type else None
            fields.append({
                "name":      f.name,
                "number":    f.number,
                "type":      f.type,
                "type_name": type_name,
                "repeated":  f.label == FieldDescriptorProto.LABEL_REPEATED,
                "enum_values": (
                    [v.name for v in f.enum_type.values]
                    if f.type == FieldDescriptorProto.TYPE_ENUM and f.enum_type
                    else []
                ),
            })
        return {"name": msg_desc.full_name, "fields": fields}

    # ──────────────────────────────────────────────────────────────────────
    # RPC invocation — unary
    # ──────────────────────────────────────────────────────────────────────

    def invoke_rpc(
        self,
        method_path:  str,
        request_json: str,
        auth_header:  Optional[str] = None,
        timeout:      Optional[float] = None,
    ) -> Any:
        service_name, method_name = self._parse_method_path(method_path)
        self._resolve_service(service_name)
        svc_desc    = self.pool.FindServiceByName(service_name)
        method_desc = svc_desc.FindMethodByName(method_name)

        input_class, output_class = self._make_message_classes(method_desc)
        request                   = Parse(request_json, input_class())
        metadata                  = [("authorization", auth_header)] if auth_header else []
        full_path                 = f"/{svc_desc.full_name}/{method_desc.name}"

        if method_desc.server_streaming:
            # Return iterator consumed by invoke_rpc_streaming
            raise ValueError(
                "Use invoke_rpc_streaming() for server-streaming methods"
            )

        response = self.channel.unary_unary(
            full_path,
            request_serializer=lambda x: x.SerializeToString(),
            response_deserializer=output_class.FromString,
        )(request, metadata=metadata, timeout=timeout)

        return MessageToDict(response, preserving_proto_field_name=True)

    # ──────────────────────────────────────────────────────────────────────
    # RPC invocation — streaming (generator)
    # ──────────────────────────────────────────────────────────────────────

    def invoke_rpc_streaming(
        self,
        method_path:  str,
        request_json: str,
        auth_header:  Optional[str] = None,
        timeout:      Optional[float] = None,
    ) -> Iterator[Dict[str, Any]]:
        """Generator: yields one dict per streamed response message."""
        service_name, method_name = self._parse_method_path(method_path)
        self._resolve_service(service_name)
        svc_desc    = self.pool.FindServiceByName(service_name)
        method_desc = svc_desc.FindMethodByName(method_name)

        input_class, output_class = self._make_message_classes(method_desc)
        request                   = Parse(request_json, input_class())
        metadata                  = [("authorization", auth_header)] if auth_header else []
        full_path                 = f"/{svc_desc.full_name}/{method_desc.name}"

        responses = self.channel.unary_stream(
            full_path,
            request_serializer=lambda x: x.SerializeToString(),
            response_deserializer=output_class.FromString,
        )(request, metadata=metadata, timeout=timeout)

        for response in responses:
            yield MessageToDict(response, preserving_proto_field_name=True)

    # ──────────────────────────────────────────────────────────────────────
    # Helpers
    # ──────────────────────────────────────────────────────────────────────

    @staticmethod
    def _parse_method_path(path: str):
        """'pkg.Svc/Method' or 'pkg.Svc.Method' → (service, method)"""
        if "/" in path:
            parts = path.rsplit("/", 1)
        else:
            parts = path.rsplit(".", 1)
        if len(parts) != 2:
            raise ValueError(f"Invalid method path: {path!r}")
        return parts[0], parts[1]

    def _make_message_classes(self, method_desc: Any):
        """Create dynamic Python message classes for input/output types."""
        from google.protobuf import reflection, message as _msg

        def _make(desc):
            return reflection.GeneratedProtocolMessageType(
                str(desc.name),
                (_msg.Message,),
                {"DESCRIPTOR": desc, "__module__": None},
            )

        return _make(method_desc.input_type), _make(method_desc.output_type)


# ──────────────────────────────────────────────────────────────────────────────
# Auth helpers
# ──────────────────────────────────────────────────────────────────────────────

def build_auth_header(args: argparse.Namespace) -> Optional[str]:
    if getattr(args, "auth", "none") == "basic":
        if not getattr(args, "user", None):
            print("❌ --user required for Basic auth")
            sys.exit(1)
        encoded = base64.b64encode(args.user.encode()).decode()
        return f"Basic {encoded}"
    if getattr(args, "auth", "none") == "jwt":
        if not getattr(args, "token", None):
            print("❌ --token required for JWT auth")
            sys.exit(1)
        return f"Bearer {args.token}"
    return None


def parse_method_path(target: str):
    parts = target.split(".")
    if len(parts) >= 3:
        last = parts[-1]
        if last[0].isupper() and not any(
            last.endswith(s) for s in ["Request", "Response", "Service", "API"]
        ):
            return ".".join(parts[:-1]), last
    return target, None


# ──────────────────────────────────────────────────────────────────────────────
# CLI main
# ──────────────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="gRPC reflection client",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("--server",  default="localhost:50051")
    parser.add_argument("--auth",    choices=["basic", "jwt", "none"], default="none")
    parser.add_argument("--user",    help="user:pass for Basic auth")
    parser.add_argument("--token",   help="JWT token")
    parser.add_argument("--timeout", type=float)

    sub = parser.add_subparsers(dest="command")

    # list
    lp = sub.add_parser("list")
    lp.add_argument("service", nargs="?")

    # describe
    dp = sub.add_parser("describe")
    dp.add_argument("target")

    # call
    cp = sub.add_parser("call")
    cp.add_argument("method")
    cp.add_argument("request")

    # repl  ← new
    rp = sub.add_parser("repl", help="Start interactive REPL")
    rp.add_argument("--service", default=None, help="Pre-select a service")

    args = parser.parse_args()
    if not args.command:
        parser.print_help()
        sys.exit(1)

    client = GrpcReflectionClient(args.server)

    try:
        if args.command == "list":
            _cmd_list(client, args)

        elif args.command == "describe":
            _cmd_describe(client, args)

        elif args.command == "call":
            _cmd_call(client, args)

        elif args.command == "repl":
            auth = build_auth_header(args)
            # Import here to avoid pulling in prompt_toolkit for non-repl use
            import sys
            sys.path.insert(0, ".")
            from repl.tea.runtime import ReplRuntime
            ReplRuntime(
                client=client,
                server=args.server,
                auth_header=auth,
                initial_service=getattr(args, "service", None),
            ).run()

    except grpc.RpcError as e:
        print(f"❌ RPC Error: {e.code()}\n   Details: {e.details()}")
        sys.exit(1)
    except KeyboardInterrupt:
        print("\n⚠️  Interrupted")
        sys.exit(0)
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback; traceback.print_exc()
        sys.exit(1)
    finally:
        client.close()


# ──────────────────────────────────────────────────────────────────────────────
# Sub-command implementations (unchanged from original)
# ──────────────────────────────────────────────────────────────────────────────

def _cmd_list(client: GrpcReflectionClient, args: argparse.Namespace) -> None:
    if getattr(args, "service", None):
        info = client.describe_service(args.service)
        print(f"📋 Service: {info['name']}\n")
        for m in info["methods"]:
            s = ""
            if m["server_streaming"]: s += "server-streaming "
            if m["client_streaming"]: s += "client-streaming"
            s = f" ({s.strip()})" if s else ""
            print(f"  • {m['name']}{s}")
            print(f"    In:  {m['input_type']}")
            print(f"    Out: {m['output_type']}")
            if m.get("options"):
                for k, v in m["options"].items():
                    print(f"    Opt: {k.split('.')[-1]} = {v}")
            print()
    else:
        services = client.list_services()
        print("📋 Available services:")
        for s in services:
            print(f"  • {s}")


def _cmd_describe(client: GrpcReflectionClient, args: argparse.Namespace) -> None:
    service_name, method_name = parse_method_path(args.target)
    try:
        info = client.describe_service(service_name)
        if method_name:
            for m in info["methods"]:
                if m["name"] == method_name:
                    print(f"📋 {service_name}.{method_name}")
                    if m["server_streaming"]: print("  [server-streaming]")
                    print(f"  In:  {m['input_type']}")
                    print(f"  Out: {m['output_type']}")
                    return
            print(f"❌ Method {method_name} not found")
        else:
            print(f"📋 Service: {info['name']}")
            for m in info["methods"]:
                print(f"  • {m['name']}")
    except Exception:
        info = client.describe_message(args.target)
        print(f"📋 Message: {info['name']}")
        for f in info["fields"]:
            t = f.get("type_name") or FIELD_TYPE_NAMES.get(f["type"], "?")
            print(f"  • {f['name']}: {t}")


def _cmd_call(client: GrpcReflectionClient, args: argparse.Namespace) -> None:
    auth = build_auth_header(args)
    print(f"🚀 {args.method}")
    service_name, method_name = GrpcReflectionClient._parse_method_path(args.method)
    client._resolve_service(service_name)
    svc_desc    = client.pool.FindServiceByName(service_name)
    method_desc = svc_desc.FindMethodByName(method_name)

    if method_desc.server_streaming:
        for chunk in client.invoke_rpc_streaming(
            args.method, args.request, auth, timeout=args.timeout
        ):
            print(f"📦 {json.dumps(chunk, indent=2)}")
    else:
        result = client.invoke_rpc(
            args.method, args.request, auth, timeout=args.timeout
        )
        print(f"📥 {json.dumps(result, indent=2)}")


if __name__ == "__main__":
    main()
