

class FlowMapper(object):
    def __init__(self, term_manager, flows):
        """
        The idea here
        :param term_manager: A TermManager to which input flows are being mapped
        :param flows: iterable of flows to be mapped
        """
        self._tm = term_manager

        self.miss = []
        self.dup = []
        self.hit = dict()
        self.cutoff = []
        self.count = 0

        for x in flows:
            self.count += 1
            if x.entity_type == 'flow':
                flow = x
                term = x.context
            elif x.entity_type == 'exchange':
                flow = x.flow
                term = x.termination
            else:
                self.miss.append(x)
                continue

            if flow in self.hit:
                self.dup.append(x)
                continue
            if term is None:
                self.cutoff.append(x)
                continue
            try:
                mf = self.tm.get_flowable(flow.name)
                cx = self.tm[term]
                self.hit[flow] = (mf, cx)
            except KeyError:
                self.miss.append(x)

        self.unmap = []
        self.mapped = dict()
        for c_flow, match in self.hit.items():
            mf, cx = match
            ct = tuple(cx)
            try:
                tgt = next(z for z in self.tm.flows_for_flowable(mf) if z.context == ct)
                self.mapped[c_flow] = tgt
            except StopIteration:
                self.unmap.append(c_flow)

        self.stats()

    def stats(self):
        print('%5d Input Flows [%d duplicates]' % (self.count, len(self.dup)))
        print('%5d Unrecognized Flowable' % len(self.miss))
        print('%5d Cutoffs' % len(self.cutoff))
        print('%5d Unmapped Flow' % len(self.unmap))
        print('%5d Mapped Flow' % len(self.mapped))

    @property
    def tm(self):
        return self._tm
