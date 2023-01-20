import argparse
import collections
import configparser
import hashlib
from math import ceil
import os
import re
import sys
import zlib

PREFIX_LEN = 2

argparser = argparse.ArgumentParser(description="Git like Content tracker")
argsubparser = argparser.add_subparsers(title="Command", dest="command")
argsubparser.required = True

argsp = argsubparser.add_parser("init", help="Initialize a new, empty repository")
argsp.add_argument("path", metavar="directory", nargs="?", default=".", help="Where to create the repository.")

argsp = argsubparser.add_parser("cat-file", help="Provided content of repository objects")
argsp.add_argument("type", metavar="type", choices=["blob","commit","tag","tree"], help="Specify a type")
argsp.add_argument("object", metavar="object", help="The object to display")

argsp = argsubparser.add_parser("hash-object", help="Compute object ID and optionally creates a blob from a file")
argsp.add_argument("-t", metavar="type", dest="type", choices=["blob", "commit", "tag", "tree"], default="blob", help="Specify the type")
argsp.add_argument("-w", dest="write", action="store_true", help="Actually write the object into the database")
argsp.add_argument("path", help="Read object from <file>")

argsp = argsubparser.add_parser("log", help="Display history of a given commit.")
argsp.add_argument("commit", default="HEAD", nargs="?", help="Commit to start at.")

argsp = argsubparser.add_parser("ls-tree", help="Pretty-print a tree object")
argsp.add_argument("object", help="The object to show")

argsp = argsubparser.add_parser("checkout", help="Checkout a commit inside a directory.")
argsp.add_argument("commit", help="The commit or tree to checkout.")
argsp.add_argument("path", help="The EMPTY directory to checkout on.")

argsp = argsubparser.add_parser("show-ref", help="List references.")

argsp = argsubparser.add_parser("tag", help="List and create tags.")
argsp.add_argument("-a", action="store_true", dest="create_tag_object", help="Whether to create a tag object")
argsp.add_argument("name", nargs="?", help="The new tag's name")
argsp.add_argument("object", default="HEAD", nargs="?", help="The object the new tag will point to")

argsp = argsubparser.add_parser("rev-parse", help="Parse revision (or other objects) identifiers")
argsp.add_argument("--wyag-type", metavar="type", dest="type", choices=["blob", "commit", "tag", "tree"], default=None, help="Specify the expected type")
argsp.add_argument("name", help="The name to parse")

argsp = argsubparser.add_parser("ls-files", help="List all the stage files")


def main(argv=sys.argv[1:]):
    args = argparser.parse_args(argv)
    
    if args.command == "init": cmd_init(args)
    elif args.command == "cat-file": cmd_cat_file(args)
    elif args.command == "hash-object": cmd_hash_object(args)
    elif args.command == "log": cmd_log(args)
    elif args.command == "ls-tree": cmd_ls_tree(args)
    elif args.command == "checkout": cmd_checkout(args)
    elif args.command == "show-ref": cmd_show_ref(args)
    elif args.command == "tag": cmd_tag(args)
    elif args.command == "rev-parse": cmd_rev_parse(args)
    elif args.command == "ls-files": cmd_ls_files(args)
    
    else: print("Unknown command")
    
def cmd_init(args): repo_create(args.path)

def cmd_cat_file(args):
    repo = repo_find()
    cat_file(repo, args.object, fmt=args.type.encode())

def cat_file(repo, obj, fmt=None):
    obj = object_read(repo, object_find(repo, obj, fmt=fmt))
    sys.stdout.buffer.write(obj.serialize())

def cmd_hash_object(args):
    repo = None
    if args.write:
        repo = GitRepository(".")
    
    with open(args.path, "rb") as fd:
        sha = object_hash(fd, args.type.encode(), repo)
        print(sha)
        
def cmd_log(args):
    repo = repo_find()
    
    print("digraph wyaglog {")
    log_graphviz(repo, object_find(repo, args.commit), set())
    print("}")

def cmd_ls_tree(args):
    repo = repo_find()
    obj = object_read(repo, object_find(repo, args.object, fmt=b'tree'))
    
    for item in obj.items:
        mode = item.mode.decode('ascii').rjust(6, '0')
        object = object_read(repo, item.sha)
        print(f"{mode} {object} {item.sha}\t{item.path.decode('ascii')}")

def cmd_checkout(args):
    repo = repo_find()
    
    obj = object_read(repo, object_find(repo, args.commit))
    
    # If object == commit, grab its tree
    if obj.fmt == b'commit':
        obj = object_read(repo, obj.kvlm[b'tree'].decode("ascii"))
    
    if os.path.exists(args.path):
        if not os.path.isdir(args.path):
            raise Exception(f"Not a directory {args.path}")
        if os.listdir(args.path):
            raise Exception(f"Not empty {args.path}!")
    else:
        os.makedirs(args.path)
    
    tree_checkout(repo, obj, os.path.realpath(args.path).encode())

def cmd_show_ref(args):
    repo = repo_find()
    refs = ref_list(repo)
    show_ref(repo, refs, prefix="refs")

def cmd_tag(args):
    repo = repo_find()
    
    if args.name:
        type = "object" if args.create_tag_object else "ref"
        tag_create(repo, args.name, args.object, type=type)
        return
    refs = ref_list(repo)
    show_ref(repo, refs["tags"], with_hash=False)

def cmd_rev_parse(args):
    fmt = None
    if args.type:
        fmt = args.type.encode()
    
    repo = repo_find()
    
    print(object_find(repo, args.name, fmt, follow=True))

def cmd_ls_files(args):
    repo = repo_find()
    for e in GitIndex(os.path.join(repo.gitdir, 'index')).entries:
        print(e.name.decode("utf8"))

def tag_create(repo, name, reference, type):
    sha = object_find(repo, reference)
    tag_sha = sha
    if type=="object":
        tag = GitTag(repo)
        tag.kvlm = collections.OrderedDict()
        tag.kvlm[b'object'] = sha.encode()
        tag.kvlm[b'type'] = b'commit'
        tag.kvlm[b'tag'] = name.encode()
        tag.kvlm[b'tagger'] = b'User <user@example.com>'
        
        tag.kvlm[b''] = b'A tag generated by wyag.'
        tag_sha = object_write(tag, repo)
        
    ref_create(repo, f"tags/{name}", tag_sha)

def ref_create(repo, ref_name, sha):
    with open(repo_file(repo, f"refs/{ref_name}"), "w") as fp:
        fp.write(sha+"\n")

def show_ref(repo, refs, with_hash=True, prefix=""):
    for k, v in refs.items():
        if type(v) == str:
            print("{0}{1}{2}".format(
                v + " " if with_hash else "",
                prefix + "/" if prefix else "",
                k
            ))
            continue
        show_ref(repo, v, with_hash=with_hash, prefix=f"{prefix}{'/' if prefix else ''}{k}")
  
def tree_checkout(repo, tree, path):
    for item in tree.items:
        obj = object_read(repo, item.sha)
        dest = os.path.join(path, item.path)
        
        if obj.fmt == b'tree':
            os.mkdir(dest)
            tree_checkout(repo, obj, dest)
        elif obj.fmt == b'blob':
            with open(dest, 'wb') as f:
                f.write(obj.blobdata)

def log_graphviz(repo, sha, seen):
    if sha in seen:
        return
    seen.add(sha)
    
    commit = object_read(repo, sha)
    assert (commit.fmt==b'commit')
    
    if not b'parent' in commit.kvlm.keys():
        return
    
    parents = commit.kvlm[b'parent']
    
    if type(parents) != list:
        parents = [ parents ]

    for p in parents:
        p = p.decode("ascii")
        print(f"c_{sha} -> c_{p}")
        log_graphviz(repo, p, seen)

def object_hash(fd, fmt, repo=None):
    data = fd.read()
    
    if   fmt==b'commit'   : obj=GitCommit(repo, data)
    elif fmt==b'tree'     : obj=GitTree(repo, data)
    elif fmt==b'tag'      : obj=GitTag(repo, data)
    elif fmt==b'blob'     : obj=GitBlob(repo, data)
    else:
        raise Exception(f"Unknown type {fmt}")
    return object_write(obj, repo)

def repo_path(repo, *path):
    return os.path.join(repo.gitdir, *path)

def repo_file(repo, *path, mkdir=False):
    if repo_dir(repo, *path[:-1], mkdir=mkdir):
        return repo_path(repo, *path)

def repo_dir(repo, *path, mkdir=False):
    path = repo_path(repo, *path)
    if os.path.exists(path):
        if os.path.isdir(path):
            return path
        raise Exception(f"Not a directory {path}")
    
    if mkdir:
        os.makedirs(path)
        return path

def repo_create(path):
    repo = GitRepository(path, True)
    
    if os.path.exists(repo.worktree):
        if not os.path.isdir(repo.worktree):
            raise Exception(f"{path} is not a dir")
        if os.listdir(repo.worktree):
            raise Exception(f"{path} is not empty")
    else:
        os.makedirs(repo.worktree)
    
    assert(repo_dir(repo, "branches", mkdir=True))
    assert(repo_dir(repo, "objects", mkdir=True))
    assert(repo_dir(repo, "refs", "tags", mkdir=True))
    assert(repo_dir(repo, "refs", "heads", mkdir=True))
    
    with open(repo_file(repo, "description"), "w") as f:
        f.write("Unnamed repository; edit tis file 'description' to name the repository.\n")
        
    with open(repo_file(repo, "HEAD"), "w") as f:
        f.write("ref: refs/head/master\n")
        
    
    with open(repo_file(repo, "config"), "w") as f:
        config = repo_default_config()
        config.write(f)
    return repo

def repo_find(path=".", required=True):
    path = os.path.realpath(path)
    
    if os.path.isdir(os.path.join(path, ".git")):
        return GitRepository(path)
    
    parent = os.path.realpath(os.path.join(path, ".."))
    
    # Bottom case
    if parent == path:
        if required:
            raise Exception("No git directory.")
        return None

    return repo_find(parent, required)

def repo_default_config():
    ret = configparser.ConfigParser()
    
    ret.add_section("core")
    ret.set("core", "repositoryformatversion", "0")
    ret.set("core", "filemode", "false")
    ret.set("core", "bare", "false")
    
    return ret

def object_read(repo, sha):
    """Read object id from Git repo. Returns GitObject"""
    path = repo_file(repo, "objects", sha[0:2], sha[2:])
    with open(path, "rb") as f:
        raw = zlib.decompress(f.read())
        
        # Read object type
        x = raw.find(b' ')
        fmt = raw[0:x] # type
        
        # Read and validate object size
        y = raw.find(b'\x00', x)
        size = int(raw[x:y].decode("ascii"))
        if size != len(raw)-y-1:
            raise Exception(f"Malformed object {sha}: bad length")
        
        # Pick constructor
        if fmt==b'commit': c=GitCommit
        elif fmt==b'tree': c=GitTree
        elif fmt==b'tag': c=GitTag
        elif fmt==b'blob': c=GitBlob
        else:
            raise Exception(f"Unknown type {fmt.decode('ascii')} for object {sha}.")

        return c(repo, raw[y+1:])

def object_find(repo, name, fmt=None, follow=True):
    sha = object_resolve(repo, name)
    
    if not sha:
        raise Exception(f"Not such reference {name}")

    if len(sha) > 1:
        raise Exception(f"Ambiguous reference {name}: Candidates are {' - '.join(sha)}")
    
    sha = sha[0]
    
    if not fmt:
        return sha
    
    while True:
        obj = object_read(repo, sha)
        
        if obj.fmt == fmt:
            return sha
        
        if not follow:
            return None
        
        if obj.fmt == b'tag':
            sha = obj.kvlm[b'object'].decode("ascii")
        elif obj.fmt == b'commit' and fmt == b'tree':
            sha = obj.kvlm[b'tree'].decode("ascii")
        else:
            return None
        
def object_write(obj, actually_write=True):
    # Serialize object data
    data = obj.serialize()
    # Add header
    result = obj.fmt + b' ' + str(len(data)).encode() + b'\x00' + data
    # Compute hash
    sha = hashlib.sha1(result).hexdigest()
    
    if actually_write:
        # Compute path
        path = repo_file(obj.repo, "objects", sha[0:2], sha[2:], mkdir=actually_write)
        
        with open(path, 'wb') as f:
            # Compress and write
            f.write(zlib.compress(result))
    return sha

def object_resolve(repo, name):
    candidates = list()
    hashRE = re.compile(r"^[0-9A-Fa-f]{4,40}$")
    
    if not name.strip(): return None
    
    if name == "HEAD":
        return [ref_resolve(repo, "HEAD")]
    
    if hashRE.match(name):
        if len(name) == 40:
            return [name.lower()]
        name = name.lower()
        prefix = name[0:PREFIX_LEN]
        path = repo_dir(repo, "objects", prefix, mkdir=False)
        if path:
            rem = name[PREFIX_LEN:]
            for f in os.listdir(path):
                if f.startswith(rem):
                    candidates.append(prefix + f)
    return candidates

class GitRepository():
    worktree = None
    gitdir = None
    conf = None
    
    def __init__(self, path, force=False):
        self.worktree = path
        self.gitdir = os.path.join(path, ".git")

        if not (force or os.path.isdir(self.gitdir)):
            raise Exception(f"Not a Git repository {path}")

        # Read config file in .git/config
        self.conf = configparser.ConfigParser()
        cf = repo_file(self, "config")
        if cf and os.path.exists(cf):
            self.conf.read([cf])
        elif not force:
            raise Exception("Configuration file missing")
        
        if not force:
            vers = int(self.conf.get("core", "repositoryformatversion"))
            if vers != 0:
                raise Exception(f"Unsupported repositoryformatversion {vers}")

class GitObject():
    
    repo = None
    
    def __init__(self, repo, data=None):
        self.repo = repo
        
        if data is not None:
            self.deserialize(data)
    
    def serialize(self):
        raise Exception("Not implmented")
    
    def deserialize(self):
        raise Exception("Not implmented")
    

class GitBlob(GitObject):
    fmt=b'blob'
    
    def serialize(self):
        return self.blobdata
    
    def deserialize(self, data):
        self.blobdata = data

class GitCommit(GitObject):
    fmt=b'commit'
    
    def serialize(self):
        return kvlm_serialize(self.kvlm)

    def deserialize(self, data):
        self.kvlm = kvlm_parse(data)


class GitTree(GitObject):
    fmt=b'tree'
    
    def serialize(self):
        return tree_serializer(self)
    
    def deserialize(self, data):
        self.items = tree_parse(data)

        

class GitTag(GitCommit):
    fmt=b'tag'

def kvlm_parse(raw, start=0, dct=None):
    if not dct:
        dct = collections.OrderedDict()
    
    # Next space and newline
    spc = raw.find(b' ', start)
    nl = raw.find(b'\n', start)
    
    # If space before newline: KEYWORD
    
    # Base case
    if (spc < 0) or (nl < spc):
        assert(nl == start)
        dct[b''] = raw[start+1:]
        return dct
    
    key = raw[start:spc]
    
    end = start
    
    # Find the end of the values. Continuation lines begin with a
    # space, so we loop iuntil we find a "\n" not followed by space
    while True:
        end = raw.find(b'\n', end+1)
        if raw[end+1] != ord(' '): break
        
    
    # Grab the value
    # Also, drop the leading space on continuation lines
    value = raw[spc+1:end].replace(b'\n ', b'\n')
    
    if key in dct:
        if type(dct[key]) == list:
            dct[key].append(value)
        else:dct[key] = [ dct[key], value]
    else:
        dct[key] = value
        
    return kvlm_parse(raw, start=end+1, dct=dct)

def kvlm_serialize(kvlm):
    ret = b''
    
    # Output fields
    for k in kvlm.keys():
        # Skip the message itself
        if k == b'': continue
        val = kvlm[k]
        
        #Normalize to a list
        if type(val) != list:
            val = [val]
        
        for v in val:
            ret += k + b' ' + (v.replace(b'\n', b'\n ')) + b'\n'
        
        # Append message
        ret += b'\n' + kvlm[b'']
        
        return ret
    
class GitTreeLeaf:
    def __init__(self, mode, path, sha):
        self.mode = mode
        self.path = path
        self.sha = sha
    
def tree_parse_one(raw, start=0):
    
    SHA_BYTES_LEN = 20
    
    # Find space terminator of mode
    x = raw.find(b' ', start)
    assert(x-start == 5 or x-start==6)

    
    mode = raw[start:x]
        
    # FInd the null terminator
    y = raw.find(b'\x00', x)
    
    path = raw[x+1:y]
    
    sha = hex(
        int.from_bytes(
            raw[y+1:y+SHA_BYTES_LEN+1], "big"))[2:]
    return y+SHA_BYTES_LEN+1, GitTreeLeaf(mode, path, sha)

def tree_parse(raw):
    pos = 0
    max = len(raw)
    ret = list()
    
    while pos < max:
        pos, data = tree_parse_one(raw, pos)
        ret.append(data)
    return ret

def tree_serializer(obj):
    ret = b''
    for i in obj.items:
        ret += i.mode
        ret += b' '
        ret += i.path
        ret += b'\x00'
        sha = int(i.sha ,16)
        ret += sha.to_bytes(20, byteorder="big")
    return ret

def ref_resolve(repo, ref):
    with open(repo_file(repo, ref), 'r') as fp:
        data = fp.read()[:-1]
        # Drop \n
    start_str = "ref: "
    if data.startswith(start_str):
        return ref_resolve(repo, data[len(start_str):])
    return data

def ref_list(repo, path=None):
    if not path:
        path = repo_dir(repo, "refs")
    ret = collections.OrderedDict()
    
    for f in sorted(os.listdir(path)):
        can = os.path.join(path, f)
        if os.path.isdir(can):
            ret[f] = ref_list(repo, can)
            continue
        ret[f] = ref_resolve(repo, can)
    return ret

class GitIndexEntry:
    def __init__(self, ctime=None, mtime=None, dev=None, ino=None,
                 mode_type=None, mode_perms=None, uid=None, gid=None,
                 fsize=None, object_hash=None, flag_assume_valid=None,
                 flag_extended=None, flag_stage=None,
                 flag_name_length=None, name=None):
        """The last time a file's metadata changed.  This is a tuple (seconds, nanoseconds)"""
        self.ctime = ctime
        """The last time a file's data changed.  This is a tuple (seconds, nanoseconds)"""
        self.mtime = mtime
        """The ID of device containing this file"""
        self.dev = dev
        """The file's inode number"""
        self.ino = ino
        """The object type, either b1000 (regular), b1010 (symlink), b1110 (gitlink). """
        self.mode_type = mode_type
        """The object permissions, an integer."""
        self.mode_perms = mode_perms
        """User ID of owner"""
        self.uid = uid
        """Group ID of ownner (according to stat 2.  Isn'th)"""
        self.gid = gid
        """Size of this object, in bytes"""
        self.fsize = fsize
        """The object's hash as a hex string"""
        self.object_hash = object_hash
        self.flag_assume_valid = flag_assume_valid
        self.flag_extended = flag_extended
        self.flag_stage = flag_stage
        """Length of the name if < 0xFFF (yes, three Fs), -1 otherwise"""
        self.flag_name_length = flag_name_length
        self.name = name

class GitIndex:
    signature = None
    version = None
    entries = []
    
    def __init__(self, file):
        raw = None
        with open(file, 'rb') as f:
            raw = f.read()
        
        header = raw[:12]
        self.signature = header[:4]
        self.version = hex(int.from_bytes(header[4:8], "big"))
        nindex = int.from_bytes(header[8:12], "big")
        
        self.entries = list()
        
        content = raw[12:]
        idx = 0
        for i in range(0, nindex):
            ctime= content[idx:idx+8]
            mtime = content[idx+8:idx+16]
            dev = content[idx+16:idx+20]
            ino = content[idx+20: idx+24]
            mode= content[idx+24:idx+28]  # TODO
            uid = content[idx+28: idx+32]
            gid = content[idx+32: idx+36]
            fsize = content[idx+36: idx+40]
            object_hash = content[idx+40: idx+60]
            flag = content[idx+60: idx+62] # TODO
            null_idx = content.find(b'\x00', idx+62) # TODO
            name = content[idx+62: null_idx]
            
            idx = null_idx+1
            idx = 8*ceil(idx/8)
            
            self.entries.append(
                GitIndexEntry(ctime=ctime, mtime=mtime, dev=dev, ino=ino, mode_type=mode, uid=uid, gid=gid, fsize=fsize, object_hash=object_hash, name=name)
            )
 