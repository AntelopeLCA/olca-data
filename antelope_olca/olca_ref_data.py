"""
supplies an Antelope archive that contains the ref data.
"""

from antelope_core.archives import BasicArchive
from antelope_core.lcia_engine import LciaEngine

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
    def make_interface(self, iface):
        if iface == 'quantity':
            return OlcaRefQuantityImplementation(self)
        return super(OpenLcaRefData, self).make_interface(iface)
