import typing as t

class NeptuneNode():
    """ A class representing a node in Neptune
    """

    def __init__(
        self,
        id: str,
        labels: t.Optional[t.Iterable[str]] = None,
        properties: t.Optional[t.Dict[str, t.Any]] = None
    ) -> None:
        self.id = id
        self.properties = frozenset(properties or ())
        self.labels = frozenset(labels or ())

    def __repr__(self) -> str:
        return (f"<Node ~id={self._element_id!r} "
                f"~labels={self._labels!r} ~properties={self._properties!r}>")

    @property
    def labels(self) -> t.FrozenSet[str]:
        """ The set of labels attached to this node.
        """
        return self._labels
    
    def get(self, name: str, default: t.Optional[object] = None) -> t.Any:
        """ Get a property value by name, optionally with a default.
        """
        return self.properties.get(name, default)

    def keys(self) -> t.KeysView[str]:
        """ Return an iterable of all property names.
        """
        return self.properties.keys()

    def values(self) -> t.ValuesView[t.Any]:
        """ Return an iterable of all property values.
        """
        return self.properties.values()

    def items(self) -> t.ItemsView[str, t.Any]:
        """ Return an iterable of all property name-value pairs.
        """
        return self.properties.items()
    

class NeptuneRelationship():
    """ A class representing a relationship in Neptune
    """

    def __init__(
        self,
        id: str,
        start: str,
        end :str,
        type: t.Optional[str] = None,
        properties: t.Optional[t.Dict[str, t.Any]] = None
    ) -> None:
        self.id = id
        self.start = start
        self.end = end
        self.properties = frozenset(properties or ())
        self.type = type

    def __repr__(self) -> str:
        return (f"<Relationship ~id={self.id!r} "
                f"~type={self.labels!r} ~properties={self._properties!r}>")

    @property
    def nodes(self) -> t.Tuple[t.Optional[NeptuneNode], t.Optional[NeptuneNode]]:
        """ The pair of nodes which this relationship connects.
        """
        return self.start, self.end

    @property
    def start(self) -> t.Optional[NeptuneNode]:
        """ The start node of this relationship.
        """
        return self.start

    @property
    def end(self) -> t.Optional[NeptuneNode]:
        """ The end node of this relationship.
        """
        return self.end

    @property
    def type(self) -> str:
        """ The type name of this relationship.
        """
        return type(self).type
    
    def get(self, name: str, default: t.Optional[object] = None) -> t.Any:
        """ Get a property value by name, optionally with a default.
        """
        return self.properties.get(name, default)

    def keys(self) -> t.KeysView[str]:
        """ Return an iterable of all property names.
        """
        return self.properties.keys()

    def values(self) -> t.ValuesView[t.Any]:
        """ Return an iterable of all property values.
        """
        return self.properties.values()

    def items(self) -> t.ItemsView[str, t.Any]:
        """ Return an iterable of all property name-value pairs.
        """
        return self.properties.items()
