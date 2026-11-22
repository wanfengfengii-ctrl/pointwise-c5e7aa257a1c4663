"""strain — 水闸夜班应变读数归档。

一卷一批：ingest 按原序建首版和底，correct 以本批现行版为父整组按原位改值和
出处，promote 换底至已收后继版。目录(catalog.json)与唯一实体槽(slot.json)
独立持久提交；覆盖前依据(basis.json)先于槽写落盘——该版及后继从槽前推，旧版
靠覆盖前持久依据恢复。换底失答留下待办，只准原单(q, r)续办，异目标拒绝；换
底中照常领票。目录确认实写才发首执，同号同目标重开可续办或复领。

行 = (正整数原位, 整数微应变或 None, 非空出处)。
"""

from __future__ import annotations

import contextlib
import json
import os
import sys
import tempfile
import threading

try:
    import fcntl
except ImportError:  # pragma: no cover - 非 POSIX 平台退化为进程内串行
    fcntl = None

__all__ = ["Archive", "Reject"]

PENDING = "pending"
_STOPS = (None, "before", "after", "confirm")


class Reject(ValueError):
    """整笔拒绝：错批、错父、重位、非法行；整窗拒领：未知票、越出票窗、依据不足。"""


# ---------------------------------------------------------------- 校验

def _key(ident):
    """批号/票号：非布尔整数或非空字符串，返规范键。"""
    if isinstance(ident, bool):
        raise Reject("非法标识")
    if isinstance(ident, int):
        return ("i", ident)
    if isinstance(ident, str) and ident:
        return ("s", ident)
    raise Reject("非法标识")


def _rows_checked(rows):
    """非空行组；每行(正整数原位, 整数或None, 非空出处)，原位不重。"""
    if not isinstance(rows, (list, tuple)) or not rows:
        raise Reject("非法行")
    out, seen = [], set()
    for row in rows:
        if not isinstance(row, (list, tuple)) or len(row) != 3:
            raise Reject("非法行")
        pos, val, src = row
        if isinstance(pos, bool) or not isinstance(pos, int) or pos < 1:
            raise Reject("非法行")
        if val is not None and (isinstance(val, bool) or not isinstance(val, int)):
            raise Reject("非法行")
        if not isinstance(src, str) or not src:
            raise Reject("非法行")
        if pos in seen:
            raise Reject("重位")
        seen.add(pos)
        out.append([pos, val, src])
    return out


def _rev_checked(rev):
    if isinstance(rev, bool) or not isinstance(rev, int) or rev < 1:
        raise Reject("非法版号")
    return rev


def _window_checked(low, high):
    """合法闭窗：1 <= l <= h 的整数。"""
    if (isinstance(low, bool) or not isinstance(low, int)
            or isinstance(high, bool) or not isinstance(high, int)
            or low < 1 or high < low):
        raise Reject("非法闭窗")
    return low, high


# ---------------------------------------------------------------- 持久化

def _read_json(path, default):
    try:
        with open(path, "rb") as fh:
            return json.load(fh)
    except FileNotFoundError:
        return default


def _write_json(path, payload):
    """原子实写：临时文件 + fsync + 换名 + 目录 fsync。"""
    data = json.dumps(payload, ensure_ascii=False, indent=1).encode("utf-8")
    tmp = "{}.tmp".format(path)
    with open(tmp, "wb") as fh:
        fh.write(data)
        fh.flush()
        os.fsync(fh.fileno())
    os.replace(tmp, path)
    dfd = os.open(os.path.dirname(path) or ".", os.O_RDONLY)
    try:
        os.fsync(dfd)
    finally:
        os.close(dfd)


class Archive:
    """应变读数归档。root 下目录、唯一实体槽、覆盖前依据各自独立持久提交。"""

    def __init__(self, root):
        self.root = os.fspath(root)
        os.makedirs(self.root, exist_ok=True)
        self._catalog_path = os.path.join(self.root, "catalog.json")
        self._slot_path = os.path.join(self.root, "slot.json")
        self._basis_path = os.path.join(self.root, "basis.json")
        self._mu = threading.RLock()
        self._lockfh = open(os.path.join(self.root, ".lock"), "a+b")
        self._refresh()

    def close(self):
        self._lockfh.close()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()
        return False

    # ------------------------------------------------------------ 装载

    def _refresh(self):
        """重读三处持久文件并自愈：孤儿槽/依据随目录裁剪，撕裂入档以目录重建。"""
        catalog = _read_json(self._catalog_path, {})
        self._batches = {}
        for rec in catalog.get("batches", []):
            self._batches[_key(rec["id"])] = rec
        self._tickets = {}
        for rec in catalog.get("tickets", []):
            self._tickets[_key(rec["id"])] = rec
        pend = catalog.get("pending")
        self._pending = (None if not pend else
                         {"key": _key(pend["batch"]), "id": pend["batch"],
                          "target": pend["target"]})
        self._slot = {}
        for rec in _read_json(self._slot_path, {}).get("slots", []):
            k = _key(rec["id"])
            if k in self._batches:
                self._slot[k] = {"rev": rec["rev"],
                                 "rows": [list(r) for r in rec["rows"]]}
        self._basis = {}
        for rec in _read_json(self._basis_path, {}).get("bases", []):
            k = _key(rec["id"])
            if k in self._batches:
                self._basis.setdefault(k, {})[rec["rev"]] = [
                    list(r) for r in rec["rows"]]
        for k, rec in self._batches.items():
            if k not in self._slot:  # 目录已收而槽缺：以首版前推重建底
                base = rec.get("base", 1)
                self._slot[k] = {"rev": base, "rows": self._roll(
                    rec["revisions"][0]["rows"], rec, 1, base)}
            if self._pending is None and self._slot[k]["rev"] != rec.get("base", 1):
                rec["base"] = self._slot[k]["rev"]  # 无待办时目录底对齐实体槽

    # ------------------------------------------------------------ 提交

    def _commit_catalog(self):
        _write_json(self._catalog_path, {
            "batches": list(self._batches.values()),
            "tickets": list(self._tickets.values()),
            "pending": None if self._pending is None else {
                "batch": self._pending["id"], "target": self._pending["target"]},
        })

    def _commit_slot(self):
        _write_json(self._slot_path, {"slots": [
            {"id": self._batches[k]["id"], "rev": s["rev"], "rows": s["rows"]}
            for k, s in self._slot.items()]})

    def _commit_basis(self):
        _write_json(self._basis_path, {"bases": [
            {"id": self._batches[k]["id"], "rev": rev, "rows": rows}
            for k, by_rev in self._basis.items()
            for rev, rows in by_rev.items()]})

    # ------------------------------------------------------------ 串行

    @contextlib.contextmanager
    def _txn(self, exclusive=True):
        """写入串行：进程内锁 + 跨进程文件锁；进事务先重读持久状态。"""
        self._mu.acquire()
        try:
            if fcntl is not None:
                fcntl.flock(self._lockfh.fileno(),
                            fcntl.LOCK_EX if exclusive else fcntl.LOCK_SH)
            try:
                self._refresh()
                yield
            finally:
                if fcntl is not None:
                    fcntl.flock(self._lockfh.fileno(), fcntl.LOCK_UN)
        finally:
            self._mu.release()

    # ------------------------------------------------------------ 物化

    @staticmethod
    def _roll(rows, rec, frm, to):
        """rows 为 frm 版物化行，按各修订改动前推至 to 版，保持原序。"""
        out = [list(r) for r in rows]
        index = {r[0]: i for i, r in enumerate(out)}
        revs = rec["revisions"]
        for rev in range(frm + 1, to + 1):
            for pos, val, src in revs[rev - 1].get("changes", ()):
                out[index[pos]] = [pos, val, src]
        return out

    def _materialize(self, k, rev):
        rec = self._batches[k]
        slot = self._slot[k]
        if rev >= slot["rev"]:  # 该版及后继从槽前推
            return self._roll(slot["rows"], rec, slot["rev"], rev)
        bases = self._basis.get(k, {})
        cands = [rv for rv in bases if rv <= rev]
        if not cands:
            raise Reject("依据不足")
        base = max(cands)  # 旧版靠覆盖前持久依据恢复
        return self._roll(bases[base], rec, base, rev)

    # ------------------------------------------------------------ 写

    def ingest(self, b, rows):
        """按原序建首版和底，返修订号 1；错批(已存在)、重位、非法行整笔拒绝。"""
        k = _key(b)
        new_rows = _rows_checked(rows)
        with self._txn():
            if k in self._batches:
                raise Reject("错批")
            self._batches[k] = {"id": b, "head": 1, "base": 1,
                                "revisions": [{"rows": new_rows}]}
            self._slot[k] = {"rev": 1, "rows": [list(r) for r in new_rows]}
            self._commit_slot()      # 唯一实体槽独立持久提交
            self._commit_catalog()   # 目录独立持久提交
            return 1

    def correct(self, b, parent, rows):
        """以本批现行版为父，整组按原位改值和出处，返新修订号。"""
        k = _key(b)
        new_rows = _rows_checked(rows)
        if isinstance(parent, bool) or not isinstance(parent, int):
            raise Reject("错父")
        with self._txn():
            rec = self._batches.get(k)
            if rec is None:
                raise Reject("错批")
            if parent != rec["head"]:
                raise Reject("错父")
            known = {r[0] for r in rec["revisions"][0]["rows"]}
            if any(r[0] not in known for r in new_rows):
                raise Reject("非法行")
            rev = rec["head"] + 1
            rec["revisions"].append({"changes": new_rows})
            rec["head"] = rev
            self._commit_catalog()
            return rev

    def issue(self, t, b, r, l, h):
        """冻结确切版和合法闭窗，返(票号, 首执)。同号同目标重开非首执，异目标拒绝。"""
        tk = _key(t)
        k = _key(b)
        _rev_checked(r)
        _window_checked(l, h)
        with self._txn():
            rec = self._batches.get(k)
            if rec is None:
                raise Reject("错批")
            if r > rec["head"]:
                raise Reject("未收版")
            old = self._tickets.get(tk)
            if old is not None:
                if (old["batch"] == b and old["rev"] == r
                        and old["l"] == l and old["h"] == h):
                    return t, False  # 同号同目标重开，可续办或复领
                raise Reject("异目标拒绝")
            self._tickets[tk] = {"id": t, "batch": b, "rev": r, "l": l, "h": h}
            self._commit_catalog()  # 目录确认实写才发首执
            return t, True

    def claim(self, t, l, h):
        """兑票：返(窗内原序行, 非缺测整数和)。未知票、越出票窗、依据不足整窗拒领。"""
        tk = _key(t)
        _window_checked(l, h)
        with self._txn(exclusive=False):
            ticket = self._tickets.get(tk)
            if ticket is None:
                raise Reject("未知票")
            if l < ticket["l"] or h > ticket["h"]:
                raise Reject("越出票窗")
            rows = self._materialize(_key(ticket["batch"]), ticket["rev"])
            win = [tuple(r) for r in rows if l <= r[0] <= h]
            total = sum(r[1] for r in win if r[1] is not None)
            return win, total

    def promote(self, q, r, stop=None):
        """换底至已收后继版 r。成功返新底版号；stop 模拟三处停办均返 'pending'。

        协议：覆盖前持久依据与待办先落盘 -> 覆写唯一实体槽(实体提交) -> 目录确认。
        stop='before' 覆写前停；'after' 实体提交后失答；'confirm' 目录确认回滚。
        失答留下待办，只准原单(q, r)续办，异目标拒绝；换底中照常领票。
        """
        if stop not in _STOPS:
            raise Reject("非法停点")
        k = _key(q)
        _rev_checked(r)
        with self._txn():
            rec = self._batches.get(k)
            if rec is None:
                raise Reject("错批")
            if r > rec["head"]:
                raise Reject("未收后继版")
            pend = self._pending
            if pend is not None and (pend["key"] != k or pend["target"] != r):
                raise Reject("异目标拒绝")  # 待办只准原单续办
            slot = self._slot[k]
            if pend is None and slot["rev"] >= r:
                raise Reject("非后继版")
            if slot["rev"] < r:
                # 覆盖前持久依据先落盘，再登记待办
                self._basis.setdefault(k, {})[slot["rev"]] = [
                    list(x) for x in slot["rows"]]
                self._commit_basis()
                if pend is None:
                    self._pending = {"key": k, "id": q, "target": r}
                    self._commit_catalog()
                if stop == "before":
                    return PENDING  # 覆写前停
                self._slot[k] = {"rev": r, "rows": self._roll(
                    slot["rows"], rec, slot["rev"], r)}
                self._commit_slot()  # 实体提交：版号和行同槽同读
                if stop == "after":
                    return PENDING  # 实体提交后失答
            if stop == "confirm":
                return PENDING  # 目录确认回滚
            rec["base"] = r
            self._pending = None
            self._commit_catalog()  # 目录确认实写
            return r


# ---------------------------------------------------------------- 核验

def _must_reject(fn, why):
    try:
        fn()
    except Reject:
        return
    raise AssertionError("应拒绝却放行: " + why)


def _verify_basic():
    with tempfile.TemporaryDirectory(prefix="strain-verify-") as root:
        ar = Archive(root)
        q = 7
        assert ar.ingest(q, [(1, 7, "A"), (2, 11, "B")]) == 1
        _must_reject(lambda: ar.ingest(q, [(1, 1, "x")]), "重批")
        _must_reject(lambda: ar.ingest("g2", [(1, 1, "x"), (1, 2, "y")]), "重位")
        _must_reject(lambda: ar.ingest("g3", [(0, 1, "x")]), "原位非正")
        _must_reject(lambda: ar.ingest("g4", [(1, 1.5, "x")]), "非整数微应变")
        _must_reject(lambda: ar.ingest("g5", [(1, True, "x")]), "布尔值")
        _must_reject(lambda: ar.ingest("g6", [(1, 1, "")]), "空出处")
        _must_reject(lambda: ar.ingest("g7", []), "空行组")
        assert ar.correct(q, 1, [(1, 19, "C")]) == 2
        _must_reject(lambda: ar.correct(q, 1, [(1, 20, "E")]), "错父")
        _must_reject(lambda: ar.correct("无批", 1, [(1, 1, "x")]), "错批")
        _must_reject(lambda: ar.correct(q, 2, [(9, 1, "x")]), "未知原位")
        _must_reject(lambda: ar.correct(q, 2, [(1, 1, "x"), (1, 2, "y")]),
                     "改正重位")
        assert ar.correct(q, 2, [(1, 19, "D")]) == 3
        _must_reject(lambda: ar.promote(q, 3, stop="半途"), "非法停点")
        # 目录与唯一实体槽各自独立持久提交
        assert os.path.exists(os.path.join(root, "catalog.json"))
        assert os.path.exists(os.path.join(root, "slot.json"))
        # 缺测行：和非缺测整数和
        ar.ingest("缺测", [(1, None, "X"), (2, 5, "Y")])
        ar.issue("票-缺", "缺测", 1, 1, 2)
        rows, total = ar.claim("票-缺", 1, 2)
        assert rows == [(1, None, "X"), (2, 5, "Y")] and total == 5
        ar.close()
    print("ok - 基本流程与整笔拒绝")


def _verify_issue_claim():
    with tempfile.TemporaryDirectory(prefix="strain-verify-") as root:
        ar = Archive(root)
        ar.ingest("b", [(1, 7, "A"), (2, 11, "B")])
        ar.correct("b", 1, [(1, 19, "C")])
        assert ar.issue("t1", "b", 1, 1, 2) == ("t1", True)   # 首执
        assert ar.issue("t1", "b", 1, 1, 2) == ("t1", False)  # 同号同目标重开
        _must_reject(lambda: ar.issue("t1", "b", 2, 1, 2), "异目标")
        _must_reject(lambda: ar.issue("t2", "b", 1, 2, 1), "非法闭窗")
        _must_reject(lambda: ar.issue("t2", "b", 3, 1, 2), "未收版")
        _must_reject(lambda: ar.issue("t2", "无", 1, 1, 2), "错批")
        rows, total = ar.claim("t1", 1, 2)
        assert rows == [(1, 7, "A"), (2, 11, "B")] and total == 18
        rows, total = ar.claim("t1", 2, 2)  # 票窗内子窗
        assert rows == [(2, 11, "B")] and total == 11
        _must_reject(lambda: ar.claim("t1", 1, 3), "越出票窗")
        _must_reject(lambda: ar.claim("t1", 0, 1), "非法闭窗")
        _must_reject(lambda: ar.claim("无票", 1, 1), "未知票")
        ar.close()
    print("ok - 出票/兑票与重开复领")


def _verify_stop(stop):
    with tempfile.TemporaryDirectory(prefix="strain-verify-") as root:
        ar = Archive(root)
        q = "闸-夜"
        assert ar.ingest(q, [(1, 7, "A"), (2, 11, "B")]) == 1
        assert ar.correct(q, 1, [(1, 19, "C")]) == 2
        assert ar.correct(q, 2, [(1, 19, "D")]) == 3
        assert ar.issue("票-前", q, 1, 1, 2) == ("票-前", True)
        assert ar.promote(q, 3, stop=stop) == "pending"
        # 换底中照常领票
        rows, total = ar.claim("票-前", 1, 2)
        assert total == 18 and rows[0][2] == "A"
        assert ar.issue("票-中", q, 3, 1, 2) == ("票-中", True)
        rows, total = ar.claim("票-中", 1, 2)
        assert total == 30 and rows[0][2] == "D"
        # 待办只准原单续办
        _must_reject(lambda: ar.promote(q, 2), "异目标续办")
        _must_reject(lambda: ar.promote("他批", 3), "异批续办")
        # 失答后重开（模拟重启）原单续办
        ar2 = Archive(root)
        assert ar2.promote(q, 3) == 3
        assert ar2.issue("票-中", q, 3, 1, 2) == ("票-中", False)  # 重开非首执
        _must_reject(lambda: ar2.promote(q, 2), "非后继版")
        _must_reject(lambda: ar2.promote(q, 4), "未收后继版")
        # 三代全窗合计与首行出处
        for rev, want_sum, want_src in ((1, 18, "A"), (2, 30, "C"), (3, 30, "D")):
            ticket = "票-{}".format(rev)
            assert ar2.issue(ticket, q, rev, 1, 2) == (ticket, True)
            rows, total = ar2.claim(ticket, 1, 2)
            assert total == want_sum, (stop, rev, total)
            assert rows[0][2] == want_src, (stop, rev, rows)
        # 再重开：旧版仍靠覆盖前持久依据恢复
        ar3 = Archive(root)
        rows, total = ar3.claim("票-1", 1, 2)
        assert total == 18 and rows[0][2] == "A"
        assert os.path.exists(os.path.join(root, "basis.json"))
        ar.close()
        ar2.close()
        ar3.close()
    print("ok - 停办点 {}: 待办续办、换底中领票、三代核回".format(stop))


def _verify_basis_missing():
    with tempfile.TemporaryDirectory(prefix="strain-verify-") as root:
        ar = Archive(root)
        ar.ingest("b", [(1, 7, "A"), (2, 11, "B")])
        ar.correct("b", 1, [(1, 19, "C")])
        assert ar.promote("b", 2) == 2
        ar.issue("t", "b", 1, 1, 2)
        rows, total = ar.claim("t", 1, 2)
        assert total == 18
        ar.close()
        os.unlink(os.path.join(root, "basis.json"))  # 覆盖前持久依据丢失
        ar2 = Archive(root)
        _must_reject(lambda: ar2.claim("t", 1, 2), "依据不足")
        ar2.close()
    print("ok - 依据不足整窗拒领")


def _verify():
    try:
        _verify_basic()
        _verify_issue_claim()
        for stop in ("before", "after", "confirm"):
            _verify_stop(stop)
        _verify_basis_missing()
    except Exception as exc:  # 核验失败非零退出
        print("verify FAIL: {!r}".format(exc), file=sys.stderr)
        return 1
    print("verify OK")
    return 0


# ---------------------------------------------------------------- 演示

def _demo():
    root = tempfile.mkdtemp(prefix="strain-demo-")
    print("归档目录 {}".format(root))
    ar = Archive(root)
    q = "夜班闸-1"
    r1 = ar.ingest(q, [(1, 7, "A"), (2, 11, "B")])
    print("入档 (1,7,A)(2,11,B)          -> 首版 r{}，底 r{}".format(r1, r1))
    r2 = ar.correct(q, r1, [(1, 19, "C")])
    print("首行改 19/C                   -> 修订 r{}".format(r2))
    r3 = ar.correct(q, r2, [(1, 19, "D")])
    print("首行再改 19/D                 -> 修订 r{}".format(r3))
    ans = ar.promote(q, r3, stop="after")
    print("直换末版 r{}（实体提交后失答） -> {}".format(r3, ans))
    t, first = ar.issue("票-1", q, 1, 1, 2)
    rows, total = ar.claim("票-1", 1, 2)
    print("换底中照常领票 {}(首执={})     -> 全窗合计={}".format(t, first, total))
    ar2 = Archive(root)  # 失答后重开
    done = ar2.promote(q, r3)
    print("原单续办 promote(同批同目标)   -> 新底 r{}".format(done))
    for rev, ticket, want_sum, want_src in ((1, "票-1", 18, "A"),
                                            (2, "票-2", 30, "C"),
                                            (3, "票-3", 30, "D")):
        if ticket != "票-1":
            _, first = ar2.issue(ticket, q, rev, 1, 2)
            print("出票 {} 冻结 r{} 窗[1,2](首执={})".format(ticket, rev, first))
        rows, total = ar2.claim(ticket, 1, 2)
        print("兑票 {}: 第{}代 全窗合计={} 首行出处={} 行={}".format(
            ticket, rev, total, rows[0][2], list(rows)))
        assert total == want_sum and rows[0][2] == want_src
    t, first = ar2.issue("票-3", q, 3, 1, 2)
    print("同号同目标重开 {}            -> 首执={}（复用原票）".format(t, first))
    rows, total = ar2.claim("票-3", 1, 2)
    print("复领 {}                     -> 全窗合计={}".format(t, total))
    try:
        ar2.issue("票-3", q, 2, 1, 2)
    except Reject as exc:
        print("同号异目标                   -> 拒绝（{}）".format(exc))
    ar.close()
    ar2.close()
    print("demo 完成：三代全窗合计 18、30、30，首行出处 A、C、D")
    return 0


def main(argv=None):
    argv = sys.argv if argv is None else argv
    if len(argv) != 2 or argv[1] not in ("verify", "demo"):
        print("用法: python3 strain.py verify|demo", file=sys.stderr)
        return 2
    return _verify() if argv[1] == "verify" else _demo()


if __name__ == "__main__":
    sys.exit(main())
