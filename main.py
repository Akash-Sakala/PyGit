from __future__ import annotations
import argparse
import hashlib
import zlib
import json
from pathlib import Path
from typing import Dict, List, Tuple
import sys
import time

# Git uses a content addressable storage system (Every object is identified by it's SHA256 hash) [But we are going to use SHA1 hash]
class GitObject:
    def __init__(self, obj_type: str, content: bytes):
        self.type = obj_type
        self.content = content

    def hash(self) -> str:
        # Hash format: f(<type> <size>\0<content>)
        header = f"{self.type} {len(self.content)}\0".encode()
        return hashlib.sha1(header + self.content).hexdigest()
    
    def serialize(self) -> bytes:
        header = f"{self.type} {len(self.content)}\0".encode()
        return zlib.compress(header + self.content) # zlib uses DEFLATE algo(LZ77 + Huffman encoding)
    
    @classmethod # Why class method? Because we are not using any instance specific data and also we are returning class
    def deserialize(cls, data: bytes) -> GitObject:
        decompressed = zlib.decompress(data)
        null_index = decompressed.find(b"\0")
        header = decompressed[:null_index].decode()
        content = decompressed[null_index+1:]

        obj_type, _ = header.split(" ")

        return cls(obj_type, content)

class Blob(GitObject):
    def __init__(self, content: bytes):
        super().__init__("blob", content)

    def get_content(self) -> bytes:
        return self.content
    
class Tree(GitObject):
    def __init__(self, entries: List[Tuple[str, str, str]]):
        self.entries = entries or []
        content = self._serialize_entries()
        super().__init__("tree", entries)
    
    def add_entry(self, mode: str, name: str, obj_hash: str):
        self.entries.append((mode, name, obj_hash))
        self.content = self._serialize_entries()

    def _serialize_entries(self) -> bytes:
        # <mode> <name>\0<hash>
        # Why sorting? consistency across hashing - hash of (hi.txt, hello.txt) is different from (hello.txt, hi.txt)
        content = b""
        for mode, name, obj_hash in sorted(self.entries):
            content += f"{mode} {name}\0".encode()
            content += bytes.fromhex(obj_hash)

        return content
    
    @classmethod
    def from_content(cls, content: bytes) -> Tree:
        tree = cls()
        i = 0

        while i < len(content):
            null_index = content.find(b"\0", i)
            if null_index == -1:
                break

            mode_name = content[i:null_index].decode()
            mode, name = mode_name.split(" ", 1)
            obj_hash = content[null_index + 1:null_index + 21].hex()
            tree.entries.append((mode, name, obj_hash))

            i = null_index + 21

        return tree
    
class Commit(GitObject):
    def __init__(self, tree_hash: str, parent_hashes: List[str], author: str, committer: str, message: str, timestamp: int = None):
        self.tree_hash = tree_hash
        self.parent_hashes = parent_hashes
        self.author = author
        self.committer = committer
        self.message = message
        self.timestamp = timestamp or int(time.time())
        content = self._serialize_commit()
        super().__init__("commit", content)

    def _serialize_commit(self):
        lines = [f"tree {self.tree_hash}"]

        for parent in self.parent_hashes:
            lines.append(f"parent {parent}")

        lines.append(f"author {self.author} {self.timestamp} +0000")
        lines.append(f"committer {self.committer} {self.timestamp} +0000")
        lines.append("")
        lines.append(self.message)

        return "\n".join(lines).encode()
    
    @classmethod
    def from_content(cls, content: bytes) -> Commit:
        lines = content.decode().split("\n")
        tree_hash = None
        parent_hashes = []
        author = None
        committer = None
        msg_start_index = 0

        for i, line in enumerate(lines):
            if line.startswith("tree "):
                tree_hash = line[5:]
            elif line.startswith("parent "):
                parent_hashes.append(line[7:])
            elif line.startswith("author "):
                author_parts = line[7:].rsplit(" ", 2)
                author = author_parts[0]
                timestamp = int(author_parts[1])
            elif line.startswith("committer "):
                committer_parts = line[10:].rsplit(" ", 2)
                committer = committer_parts[0]
            elif line == "":
                msg_start_index = i+1
                break
        
        message = "\n".join(lines[msg_start_index:])
        commit = cls(tree_hash, parent_hashes, author, committer, message, timestamp)
        return commit

class Repositry:
    def __init__(self, path = "."):
        self.path = Path(path).resolve()
        self.git_dir = self.path / ".git"

        # .git/objects
        self.objects_dir = self.git_dir / "objects"

        # .git/refs
        self.ref_dir = self.git_dir / "refs"
        self.heads_dir = self.ref_dir / "heads"

        # .git/HEAD file
        self.head_file = self.git_dir / "HEAD"

        # .git/index file
        self.index_file = self.git_dir / "index"

    def init(self) -> bool:
        if self.git_dir.exists():
            return False

        # create directories
        self.git_dir.mkdir()
        self.objects_dir.mkdir()
        self.ref_dir.mkdir()
        self.heads_dir.mkdir()

        # create initial HEAD pointing to a branch
        self.head_file.write_text("ref: refs/heads/main\n")

        self.save_index({})

        print(f"Initialized empty Git repositry in {self.git_dir}")
        return True

    def save_index(self, index: Dict[str, str]):
        self.index_file.write_text(json.dumps(index, indent=2))

    def store_object(self, obj: GitObject) -> str:
        obj_hash = obj.hash()
        obj_dir = self.objects_dir / obj_hash[:2]
        obj_file = obj_dir / obj_hash[2:]

        if not obj_file.exists():
            obj_dir.mkdir(exist_ok=True)
            obj_file.write_bytes(obj.serialize())

        return obj_hash
    
    def load_index(self) -> Dict[str, str]:
        if not self.index_file.exists():
            return {}
        
        try:
            return json.loads(self.index_file.read_text())
        except:
            return {}

    def add_file(self, path: str):
        # 1. Read the file content
        # 2. Create BLOB(Binary Large OBject - compressed file content by lossless compression) from the content
        # 3. store the blob object in Database (.git/objects)
        # 4. Update index to include the file

        full_path = self.path / path
        if not full_path.exists():
            raise FileNotFoundError("Path: {path} not found")
        
        # Read the file content
        content = full_path.read_bytes()

        # Create BLOB object from the content
        blob = Blob(content)

        # store the blob object in .git/objects
        blob_hash = self.store_object(blob)

        # Update index to include the <file_path> : <hash>
        index = self.load_index()
        index[path] = blob_hash
        self.save_index(index)

        print(f"Added {path}")

        # Clean up old blobs
        self.gc()

    def add_directory(self, path: str):
        
        # 1. Recursively traverse the directory
        # 2. Create Blob objects for all files
        # 3. Store all Blobs in the object database (.git/objects)
        # 4. Updates the index to include all the files

        full_path = self.path / path
        if not full_path.exists():
            raise FileNotFoundError("Path: {path} not found")
        if not full_path.is_dir():
            raise ValueError(f"{path} is not a directory")
        
        index = self.load_index()
        added_cnt = 0

        # Recursively traverse the directory
        # rglob("pattern") - Recursively yield all existing files (of any kind, including directories) matching the given relative pattern, anywhere in this subtree.
        for file_path in full_path.rglob("*"):
            if file_path.is_file():
                if ".git" in file_path.parts or ".vscode" in file_path.parts:
                    continue

                # Create and Store Blob object for the file
                content = file_path.read_bytes()
                blob = Blob(content)
                blob_hash = self.store_object(blob)

                # Update index 
                rel_path = str(file_path.relative_to(self.path))
                index[rel_path] = blob_hash
                added_cnt += 1

        self.save_index(index)

        # Remove old/unreferenced objects
        self.gc()

        if added_cnt > 0:
            print(f"Added {added_cnt} files from directory {path}")
        else:
            print(f"Directory {path} already up to date")

    def add_path(self, path: str) -> None:
        full_path = self.path / path

        if not full_path.exists():
            raise FileNotFoundError("Path: {path} not found")
        
        if full_path.is_file():
            self.add_file(path)
        elif full_path.is_dir():
            self.add_directory(path)
        else:
            raise ValueError(f"{path} is neither a file nor a directory")
        
    def create_tree_from_index(self):
        index = self.load_index()
        if not index:
            tree = Tree()
            return self.store_object(tree)
        
        dirs = {}
        files = {}

        for file_path, blob_hash in index.items():
            parts = file_path.split("/")

            if len(parts) == 1:
                # file is in the root folder
                files[parts[0]] = blob_hash
            else:
                dir_name = parts[0]
                if dir_name not in dirs:
                    dirs[dir_name] = {}

                curr = dirs[dir_name]
                for part in parts[1:-1]:
                    if part not in curr:
                        curr[part] = {}

                    curr = curr[part]

                curr[parts[-1]] = blob_hash

        def create_tree_recursive(entries_dict: Dict):
            tree = Tree()

            for name, value in entries_dict.items():
                if isinstance(value, str): # if value is blob_hash(str type)
                    tree.add_entry("100644", name, value)

                if isinstance(value, dict): # if value is sub directory(dict type)
                    subtree_hash = create_tree_recursive(value)
                    tree.add_entry("40000", name, subtree_hash)

            return self.store_object(tree)

        root_entries = {**files}
        for dir_name, dir_contents in dirs:
            root_entries[dir_name] = dir_contents

        return create_tree_recursive(root_entries)

    def commit(self, message: str, author: str = "PyGit user <user@pygit.com>"):
        # Create a tree object from the index( staging area)
        tree_hash = self.create_tree_from_index()

        commit = Commit(
            tree_hash=tree_hash,
            # parent_hashes=parent_hashes,
            author=author,
            committer=author,
            message=message,
            timestamp=None
        )

        

    
        
    # Garbage collector
    def gc(self):
        # Removes blobs in .git/objects that are NOT referenced in the index file

        index = self.load_index()
        used_hashes = set(index.values())

        # Iterate over .git/objects directories(named after first 2 characters of the hash)

        for obj_dir in self.objects_dir.iterdir():
            if not obj_dir.is_dir() or len(obj_dir.name) != 2:
                continue

            for obj_file in obj_dir.iterdir():
                full_hash = obj_dir.name + obj_file.name
                if full_hash not in used_hashes:
                    obj_file.unlink() # removes unused object
                    print(f"Removed unused object: {full_hash}")

            # Remove the directory if empty
            if not any(obj_dir.iterdir()):
                obj_dir.rmdir()

def main():
    parser = argparse.ArgumentParser(description="PyGit - A simple git clone")
    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # init command
    init_parser = subparsers.add_parser("init", help="Initialize a new repositry")
    
    # add command
    add_parser = subparsers.add_parser("add", help="Add files and directories to the staging area")
    add_parser.add_argument("paths", nargs="+", help="Files and directories to add")

    # commit command - Snapshot of current staging area
    commit_parser = subparsers.add_parser("commit", help="Create a new commit")
    commit_parser.add_argument("-m", "--message", help="Commit message", required=True)
    commit_parser.add_argument("--author", help="Author name and email")





    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        return
    
    repo = Repositry()
    try:
        if args.command == "init":
            if not repo.init():
                print("Repositry already exists")
                return
            
        elif args.command == "add":
            if not repo.git_dir.exists():
                print("Not a git repositry")
                return
            
            for path in args.paths:
                repo.add_path(path)


        elif args.command == "commit":
            if not repo.git_dir.exists():
                print("Not a git repositry")
                return
            
            author = args.author or "PyGit user <user@pygit.com>"
            repo.commit(args.message, author)

            
        
            
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)

main()