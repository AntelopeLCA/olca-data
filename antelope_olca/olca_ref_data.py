"""
supplies an Antelope archive that contains the ref data.
"""
import os

from antelope_core.archives import BasicArchive
from antelope_core.entities import LcFlow, LcQuantity, LcUnit
from antelope_core.providers.openlca_jsonld import pull_geog
from antelope_core.lcia_engine import LciaEngine
from .accessors import read_refdata, read_contexts, read_unit_groups, impact_methods, BASE_DIR

from .olca_ref_quantity import OlcaRefQuantityImplementation


class OpenLcaRefData(BasicArchive):
    """
    Basic idea here is to load all flows, flow properties, and impact categories and serve'em up.
    a hard-ish problem is how to allow the comprehensive listing + lazy loading of LCIA cf's-- my architecture
    is not designed to maintain a distinct index from entity list *in a single archive*

    Main thought is to override QuantityImplementation to simply check-load each method before answering any
    data-dependent queries.  Even so, things like profile() will still only report results from loaded methods.

    But maybe that is ok.
    With the impact_methods.py script, we now have a way to load the listing of LCIA quantities with proper UUIDs.

    So-- what has to happen for the subclass?  _fetch, _load_all
    """

    _ns_uuid_required = None  # don't bother with this- our entities all have UUIDs already

    def make_interface(self, iface):
        if iface == 'quantity':
            return OlcaRefQuantityImplementation(self)
        return super(OpenLcaRefData, self).make_interface(iface)

    def __init__(self, **kwargs):
        super(OpenLcaRefData, self).__init__(BASE_DIR, term_manager=LciaEngine(), ns_uuid=None, **kwargs)
        self._index = dict()
        self._olca_contexts = read_contexts()
        self._olca_unitgroups = read_unit_groups()
        self._load_flow_properties()
        # self.load_all()  for profiling reasons, keep this separate

    def _fetch(self, entity, **kwargs):
        """
        :param entity:
        :param kwargs:
        :return:
        """
        ent = self[entity]
        if ent is None:
            try:
                imp = next(impact_methods(entity))
            except StopIteration:
                raise KeyError(entity)
            return self.tm.get_canonical(imp)
        else:
            return ent

    def _load_flow_properties(self):
        for fp in read_refdata('flow_properties'):
            if self[fp['UUID']] is None:
                ug = self._olca_unitgroups[fp['UNIT_GROUP']]
                cx = self._olca_contexts[fp['CAT_UUID']]
                q = LcQuantity(fp['UUID'], Name=fp['FLOW_PROPERTY_NAME'], ReferenceUnit=LcUnit(ug['ref_unit']),
                               entity_uuid=fp['UUID'], Category=cx,
                               UnitConversion=ug['units'])
                self.add(q)

    def _load_flows(self):
        for flow in read_refdata('flows'):
            if self[flow['UUID']] is None:
                fp = self[flow['REF_QTY']]
                if fp is None:
                    raise KeyError('Flow: %s: unknown ref quantity %s' % (flow['UUID'], flow['REF_QTY']))
                cx = self.tm.add_context(self._olca_contexts[flow['CAT_UUID']])
                f = LcFlow(flow['UUID'], Name=flow['FLOW_NAME'], context=cx, FLOW_TYPE=flow['FLOW_TYPE'],
                           entity_uuid=flow['UUID'],
                           CasNumber=flow['CAS_NUMBER'], Formula=flow['FORMULA'], ReferenceQuantity=fp)
                self.add(f)

    def _load_fp_factors(self):
        for fpf in read_refdata('flow_property_factors'):
            f = self[fpf['FLOW_UUID']]
            q = self[fpf['FLOW_PROPERTY_UUID']]
            if q is f.reference_entity:
                continue

            self.tm.add_characterization(f.link, f.reference_entity, q, value=float(fpf['VALUE']), context=f.context,
                                         origin=self.ref, location=pull_geog(f.name))

    def _load_impact_methods(self):
        for im in read_refdata('impact_categories'):
            if self[im['UUID']] is None:
                u = LcUnit(im['REFERENCE_UNIT'])
                name = os.path.splitext(im['FILENAME'])[0]
                q = LcQuantity(im['UUID'], ReferenceUnit=u, Filename=im['FILENAME'], Name=name,
                               Method=im['IMPACT_METHOD'],
                               Category=im['IMPACT_CATEGORY'],
                               Indicator=im['REFERENCE_UNIT'])
                self.add(q)

    def _load_all(self, **kwargs):
        self._load_flows()
        self._load_fp_factors()
        self._load_impact_methods()
