class ContextNotFound(Exception):
    pass


class FlowNotMissing(Exception):
    pass


class TooManyDaves(Exception):
    pass


class ReferenceEntityMismatch(Exception):
    pass


class FlowMapper(object):

    @staticmethod
    def _flowof(x):
        if x.entity_type == 'exchange':
            return x.flow
        return x

    @staticmethod
    def _contextof(x):
        if x.entity_type == 'exchange':
            return x.termination or x.flow.context
        return x.context


    def _run_x(self, x, use=None):
        if x in self.hit:
            self.dup.append(x)
            return
        flow = self._flowof(x)
        term = self._contextof(x)

        if use is None:
            mf = self.tm.get_flowable(flow)  # else, raise KeyError
        else:
            mf = self.tm.get_flowable(use)
        if term is None or term == ():
            self.cutoff[x] = mf
        else:
            cx = self.tm[term]
            self.hit[x] = (mf, cx)

    def _map(self, fo, tgt):
        if fo in self.mapped:
            print('respecifying %s' % fo)
        self.mapped[fo] = tgt

    def _map_x(self, c_flow):
        """
        Attempt to map the input flow to one of the valid flows for the matching flowable.
        The intention here is to use only unambiguously correct logic, leaving any interpretive logic to client code.
        SO: the flow's canonical context must match and the reference entities must be equal (or have a 1:1 unit
        conversion)
        If multiple flows satisfy these conditions, then the targets are filtered to flows whose names match exactly.
        A mapping is added only if there is exactly one valid choice.
        :param c_flow:
        :return:
        """
        _fo = self._flowof(c_flow)
        mf, cx = self.hit[c_flow]
        tgt = list(z for z in self.tm.flows_for_flowable(mf) if self.tm[z.context] is cx)
        if len(tgt) == 0:
            raise ContextNotFound
        tgt_ref = list(z for z in tgt if z.reference_entity == _fo.reference_entity)
        if len(tgt_ref) == 0:
            tgt_unit = []
            for z in tgt:
                try:
                    cf = z.reference_entity.convert(from_unit=_fo.unit)
                except KeyError:
                    continue
                if cf == 1.0:
                    tgt_unit.append(z)
            if len(tgt_unit) == 0:
                raise ReferenceEntityMismatch
            else:
                tgt = tgt_unit
        else:
            tgt = tgt_ref
        if len(tgt) > 1:
            tgt_name = list(z for z in tgt if z.name == _fo.name)
            if len(tgt_name) > 0:
                tgt = tgt_name

        if len(tgt) == 1:
            self._map(_fo, tgt[0])
            if c_flow in self._ambiguous:
                self._ambiguous.pop(c_flow)
        else:
            self._ambiguous[c_flow] = tgt

    def __init__(self, term_manager, flows=None):
        """
        The idea here
        :param term_manager: A TermManager whose known flows are mapping targets
        :param flows: iterable of flows or exchanges to be mapped
        """
        self._tm = term_manager

        self.count = 0

        self.bad_type = []
        self.miss = []  # all flows that did not match
        self.dup = []  # inputs encountered repeatedly
        self.cutoff = dict()  # {input : flowable} when context is None or ()

        self.hit = dict()  # {input : flowable match, context match}

        self._unmap = set()  # hit entries for which context can't be mapped to flow
        self._badref = set()  # hit entries for which no matching flows have the same reference entity

        self.mapped = dict()  # {input flow: entity that the term manager knows}
        self._ambiguous = dict()  # {input: [entities] where no or multiple string names match exactly}

        if flows:

            for x in flows:
                self.map_new_flow(x)

            self.stats()

    def map_new_flow(self, x):
        if self._flowof(x) in self.mapped:
            return
        self.count += 1
        try:
            self._run_x(x)
        except TypeError:
            self.bad_type.append(x)
            return

        except KeyError:
            self.miss.append(x)

        if x in self.hit:
            self.remap(x, quiet=True)

    def map(self, x):
        _fo = self._flowof(x)
        if _fo in self.mapped:
            return self.mapped[_fo]
        elif x in self._ambiguous:
            raise TooManyDaves(_fo.name)
        elif x in self._badref:
            raise ReferenceEntityMismatch(_fo.link)
        elif x in self._unmap:
            raise ContextNotFound(self._contextof(x))
        else:
            raise KeyError

    @property
    def unmap(self):
        return sorted(self._unmap, key=lambda x: self._flowof(x).name)

    @property
    def badref(self):
        return sorted(self._badref, key=lambda x: self._flowof(x).name)

    @property
    def amb(self):
        return sorted(self._ambiguous.keys(), key=lambda x: self._flowof(x).name)

    def amb_matches(self, u):
        return self._ambiguous[u]

    @property
    def hits(self):
        for k in self.hit.keys():
            yield k

    @property
    def flows(self):
        for k in sorted(self.mapped.keys()):
            yield k

    def rerun(self, m, use=None):
        """
        Re-run m, specifying a name to use
        :param m:
        :param use:
        :return:
        """
        if m not in self.miss:
            raise FlowNotMissing(m)
        try:
            self._run_x(m, use=use)
        except KeyError:
            print('not found')
            pass
        if m in self.hit:
            self.miss.remove(m)
            self.remap(m)

    def remap(self, u, quiet=False):
        if self._flowof(u) in self.mapped:
            raise FlowNotMissing(u)
        try:
            self._map_x(u)
            if not quiet:
                print('hit %s' % u)
            if u in self._unmap:
                self._unmap.remove(u)
            if u in self._badref:
                self._badref.remove(u)
        except ContextNotFound:
            print('hit; unmap %s' % u)
            self._unmap.add(u)
        except ReferenceEntityMismatch:
            print('hit; badref %s' % u)
            self._badref.add(u)

    def stats(self):
        print('%5d Input Flows [%d duplicates]' % (self.count, len(self.dup)))
        print('%5d Unrecognized Flowable' % len(self.miss))
        print('%5d Cutoffs' % len(self.cutoff))
        print('%5d Unmapped Flow' % len(self._unmap))
        print('%5d Bad reference' % len(self._badref))
        print('%5d Mapped Flow' % len(self.mapped))
        print('%5d Ambiguous' % len(self._ambiguous))

    def map_ambiguous(self, x, t):
        """
        Use to manually resolve ambiguous matches
        :param x: an input item with an ambiguous mapping
        :param t: one of the already-established mapping candidates for x
        :return:
        """
        if x in self._ambiguous:
            if t in self._ambiguous[x]:
                _fo = self._flowof(x)
                print('%s -> %s' %(_fo.name, t.name))
                self._map(_fo, t)
                self._ambiguous.pop(x)
            else:
                raise KeyError(t)
        else:
            raise FlowNotMissing(x)

    def longest_substring(self, u=None):
        if u:
            _fo = self._flowof(u)
            return sorted((k for k in self._ambiguous[u] if _fo.name.startswith(k.name)),
                          key=lambda x: len(x.name))[-1:]
        else:
            return [self.longest_substring(am) for am in self._ambiguous.keys()]

    def shortest_superstring(self, u=None):
        if u:
            _fo = self._flowof(u)
            return sorted((k for k in self._ambiguous[u] if k.name.startswith(_fo.name)),
                          key=lambda x: len(x.name))[:1]
        else:
            return [self.shortest_superstring(am) for am in self._ambiguous.keys()]

    def first(self, u=None, n=0):
        if u:
            return self._ambiguous[u][n:n+1]
        else:
            return [self.first(am, n) for am in self._ambiguous.keys()]

    def apply(self, meth, **kwargs):
        am = list(self.amb)
        for a in am:
            try:
                self.map_ambiguous(a, meth(a, **kwargs)[0])
            except IndexError:
                pass

    @property
    def tm(self):
        return self._tm
