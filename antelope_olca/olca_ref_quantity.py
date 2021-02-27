from antelope_core.implementations import QuantityImplementation
from antelope_core.providers.openlca_jsonld import pull_geog

from .accessors import read_impact_method


CF_LOADED = set()  # store a list of impact methods that have been loaded- for lazy-loading purposes
'''
Note: we don't SAVE the literal CFs- we just keep track of which ones we've delivered
'''

def _get_impact_cfs(q):
    print('DEBUG: reading impact factors for %s' % q['Filename'])
    cfs = read_impact_method(q['Filename'])
    CF_LOADED.add(q.external_ref)
    return cfs



class OlcaRefQuantityImplementation(QuantityImplementation):
    """
    The purpose of this is just to enable lazy loading of LCIA factors
    """
    def _load_cfs(self, q):
        try:
            cfs = _get_impact_cfs(q)
        except KeyError:
            print('%s: No filename property found' % q.external_ref)
            return
        except FileNotFoundError:
            print('%s: No data file found' % q['Filename'])
            return
        if len(cfs) == 0:
            print('%s: No data' % q)
            return
        q = self._archive[cfs[0]['IMPACT_CATEGORY_UUID']]
        for cf in cfs:
            f = self[cf['FLOW_UUID']]
            cx = (cf['CATEGORY'], cf['SUBCATEGORY'])
            value = float(cf['VALUE']) * f.reference_entity.convert(to=cf['UNIT'])
            self.characterize(f.link, f.reference_entity, q, value, context=cx, location=pull_geog(f.name),
                              origin=self.origin)

    def _check_method(self, quantity):
        q = self.get_canonical(quantity)
        if q.external_ref not in CF_LOADED:
            self._load_cfs(q)
        return q

    def factors(self, quantity, **kwargs):
        q = self._check_method(self.get_canonical(quantity))
        return super(OlcaRefQuantityImplementation, self).factors(q, **kwargs)

    def quantity_relation(self, flowable, ref_quantity, query_quantity, context, **kwargs):
        q = self._check_method(self.get_canonical(query_quantity))
        return super(OlcaRefQuantityImplementation, self).quantity_relation(flowable, ref_quantity, q, context, **kwargs)

    def cf(self, flow, quantity, **kwargs):
        q = self._check_method(quantity)
        return super(OlcaRefQuantityImplementation, self).cf(flow, quantity, **kwargs)

    def do_lcia(self, quantity, inventory, **kwargs):
        q = self._check_method(quantity)
        return super(OlcaRefQuantityImplementation, self).do_lcia(q, inventory, **kwargs)
