class ContextNotFound(Exception):
    pass


class FlowNotMissing(Exception):
    pass


class TooManyDaves(Exception):
    pass


class FlowMapper(object):

    @staticmethod
    def _flowof(x):
        if x.entity_type == 'exchange':
            return x.flow
        return x

    def _run_x(self, x, use=None):
        if x in self.hit:
            self.dup.append(x)
            return

        if x.entity_type == 'flow':
            flow = x
            term = x.context
        elif x.entity_type == 'exchange':
            flow = x.flow
            term = x.termination
            if term is None:
                term = flow.context
        else:
            raise TypeError
        if use is None:
            mf = self.tm.get_flowable(flow)  # else, raise KeyError
        else:
            mf = self.tm.get_flowable(use)
        if term is None or term == ():
            self.cutoff[x] = mf
        else:
            cx = self.tm[term]
            self.hit[x] = (mf, cx)

    def _map_x(self, c_flow):
        mf, cx = self.hit[c_flow]
        ct = tuple(cx)
        tgt = list(z for z in self.tm.flows_for_flowable(mf) if z.context == ct)
        if len(tgt) == 0:
            raise ContextNotFound
        _fo = self._flowof(c_flow)
        if _fo in self.mapped:
            print('respecifying %s' % _fo)
        self.mapped[_fo] = tgt

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

        self.mapped = dict()  # {input: [flow entities] that the term manager knows}

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
            try:
                self._map_x(x)
            except ContextNotFound:
                self._unmap.add(x)

    def map(self, x, n=None):
        tgts = self.mapped[self._flowof(x)]
        if len(tgts) >1:
            if n is None:
                raise TooManyDaves(self._flowof(x))
            else:
                return tgts[n]
        elif len(tgts) == 1:
            return tgts[0]
        raise ValueError

    @property
    def unmap(self):
        return sorted(self._unmap)

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

    def remap(self, u):
        if self._flowof(u) in self.mapped:
            raise FlowNotMissing(u)
        try:
            self._map_x(u)
            print('hit %s' % u)
        except ContextNotFound:
            print('hit; unmap %s' % u)
            self._unmap.add(u)

    def stats(self):
        print('%5d Input Flows [%d duplicates]' % (self.count, len(self.dup)))
        print('%5d Unrecognized Flowable' % len(self.miss))
        print('%5d Cutoffs' % len(self.cutoff))
        print('%5d Unmapped Flow' % len(self._unmap))
        print('%5d Mapped Flow' % len(self.mapped))

    @property
    def tm(self):
        return self._tm
