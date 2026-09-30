// Code generated from GQL.g4 by ANTLR 4.13.2. DO NOT EDIT.

package parser // GQL

import "github.com/antlr4-go/antlr/v4"

// GQLListener is a complete listener for a parse tree produced by GQLParser.
type GQLListener interface {
	antlr.ParseTreeListener

	// EnterGqlProgram is called when entering the gqlProgram production.
	EnterGqlProgram(c *GqlProgramContext)

	// EnterProgramActivity is called when entering the programActivity production.
	EnterProgramActivity(c *ProgramActivityContext)

	// EnterSessionActivity is called when entering the sessionActivity production.
	EnterSessionActivity(c *SessionActivityContext)

	// EnterTransactionActivity is called when entering the transactionActivity production.
	EnterTransactionActivity(c *TransactionActivityContext)

	// EnterEndTransactionCommand is called when entering the endTransactionCommand production.
	EnterEndTransactionCommand(c *EndTransactionCommandContext)

	// EnterSessionSetCommand is called when entering the sessionSetCommand production.
	EnterSessionSetCommand(c *SessionSetCommandContext)

	// EnterSessionSetSchemaClause is called when entering the sessionSetSchemaClause production.
	EnterSessionSetSchemaClause(c *SessionSetSchemaClauseContext)

	// EnterSessionSetGraphClause is called when entering the sessionSetGraphClause production.
	EnterSessionSetGraphClause(c *SessionSetGraphClauseContext)

	// EnterSessionSetTimeZoneClause is called when entering the sessionSetTimeZoneClause production.
	EnterSessionSetTimeZoneClause(c *SessionSetTimeZoneClauseContext)

	// EnterSetTimeZoneValue is called when entering the setTimeZoneValue production.
	EnterSetTimeZoneValue(c *SetTimeZoneValueContext)

	// EnterSessionSetParameterClause is called when entering the sessionSetParameterClause production.
	EnterSessionSetParameterClause(c *SessionSetParameterClauseContext)

	// EnterSessionSetGraphParameterClause is called when entering the sessionSetGraphParameterClause production.
	EnterSessionSetGraphParameterClause(c *SessionSetGraphParameterClauseContext)

	// EnterSessionSetBindingTableParameterClause is called when entering the sessionSetBindingTableParameterClause production.
	EnterSessionSetBindingTableParameterClause(c *SessionSetBindingTableParameterClauseContext)

	// EnterSessionSetValueParameterClause is called when entering the sessionSetValueParameterClause production.
	EnterSessionSetValueParameterClause(c *SessionSetValueParameterClauseContext)

	// EnterSessionSetParameterName is called when entering the sessionSetParameterName production.
	EnterSessionSetParameterName(c *SessionSetParameterNameContext)

	// EnterSessionResetCommand is called when entering the sessionResetCommand production.
	EnterSessionResetCommand(c *SessionResetCommandContext)

	// EnterSessionResetArguments is called when entering the sessionResetArguments production.
	EnterSessionResetArguments(c *SessionResetArgumentsContext)

	// EnterSessionCloseCommand is called when entering the sessionCloseCommand production.
	EnterSessionCloseCommand(c *SessionCloseCommandContext)

	// EnterSessionParameterSpecification is called when entering the sessionParameterSpecification production.
	EnterSessionParameterSpecification(c *SessionParameterSpecificationContext)

	// EnterStartTransactionCommand is called when entering the startTransactionCommand production.
	EnterStartTransactionCommand(c *StartTransactionCommandContext)

	// EnterTransactionCharacteristics is called when entering the transactionCharacteristics production.
	EnterTransactionCharacteristics(c *TransactionCharacteristicsContext)

	// EnterTransactionMode is called when entering the transactionMode production.
	EnterTransactionMode(c *TransactionModeContext)

	// EnterTransactionAccessMode is called when entering the transactionAccessMode production.
	EnterTransactionAccessMode(c *TransactionAccessModeContext)

	// EnterRollbackCommand is called when entering the rollbackCommand production.
	EnterRollbackCommand(c *RollbackCommandContext)

	// EnterCommitCommand is called when entering the commitCommand production.
	EnterCommitCommand(c *CommitCommandContext)

	// EnterNestedProcedureSpecification is called when entering the nestedProcedureSpecification production.
	EnterNestedProcedureSpecification(c *NestedProcedureSpecificationContext)

	// EnterProcedureSpecification is called when entering the procedureSpecification production.
	EnterProcedureSpecification(c *ProcedureSpecificationContext)

	// EnterNestedDataModifyingProcedureSpecification is called when entering the nestedDataModifyingProcedureSpecification production.
	EnterNestedDataModifyingProcedureSpecification(c *NestedDataModifyingProcedureSpecificationContext)

	// EnterNestedQuerySpecification is called when entering the nestedQuerySpecification production.
	EnterNestedQuerySpecification(c *NestedQuerySpecificationContext)

	// EnterProcedureBody is called when entering the procedureBody production.
	EnterProcedureBody(c *ProcedureBodyContext)

	// EnterBindingVariableDefinitionBlock is called when entering the bindingVariableDefinitionBlock production.
	EnterBindingVariableDefinitionBlock(c *BindingVariableDefinitionBlockContext)

	// EnterBindingVariableDefinition is called when entering the bindingVariableDefinition production.
	EnterBindingVariableDefinition(c *BindingVariableDefinitionContext)

	// EnterStatementBlock is called when entering the statementBlock production.
	EnterStatementBlock(c *StatementBlockContext)

	// EnterStatement is called when entering the statement production.
	EnterStatement(c *StatementContext)

	// EnterNextStatement is called when entering the nextStatement production.
	EnterNextStatement(c *NextStatementContext)

	// EnterGraphVariableDefinition is called when entering the graphVariableDefinition production.
	EnterGraphVariableDefinition(c *GraphVariableDefinitionContext)

	// EnterOptTypedGraphInitializer is called when entering the optTypedGraphInitializer production.
	EnterOptTypedGraphInitializer(c *OptTypedGraphInitializerContext)

	// EnterGraphInitializer is called when entering the graphInitializer production.
	EnterGraphInitializer(c *GraphInitializerContext)

	// EnterBindingTableVariableDefinition is called when entering the bindingTableVariableDefinition production.
	EnterBindingTableVariableDefinition(c *BindingTableVariableDefinitionContext)

	// EnterOptTypedBindingTableInitializer is called when entering the optTypedBindingTableInitializer production.
	EnterOptTypedBindingTableInitializer(c *OptTypedBindingTableInitializerContext)

	// EnterBindingTableInitializer is called when entering the bindingTableInitializer production.
	EnterBindingTableInitializer(c *BindingTableInitializerContext)

	// EnterValueVariableDefinition is called when entering the valueVariableDefinition production.
	EnterValueVariableDefinition(c *ValueVariableDefinitionContext)

	// EnterOptTypedValueInitializer is called when entering the optTypedValueInitializer production.
	EnterOptTypedValueInitializer(c *OptTypedValueInitializerContext)

	// EnterValueInitializer is called when entering the valueInitializer production.
	EnterValueInitializer(c *ValueInitializerContext)

	// EnterGraphExpression is called when entering the graphExpression production.
	EnterGraphExpression(c *GraphExpressionContext)

	// EnterCurrentGraph is called when entering the currentGraph production.
	EnterCurrentGraph(c *CurrentGraphContext)

	// EnterBindingTableExpression is called when entering the bindingTableExpression production.
	EnterBindingTableExpression(c *BindingTableExpressionContext)

	// EnterNestedBindingTableQuerySpecification is called when entering the nestedBindingTableQuerySpecification production.
	EnterNestedBindingTableQuerySpecification(c *NestedBindingTableQuerySpecificationContext)

	// EnterObjectExpressionPrimary is called when entering the objectExpressionPrimary production.
	EnterObjectExpressionPrimary(c *ObjectExpressionPrimaryContext)

	// EnterLinearCatalogModifyingStatement is called when entering the linearCatalogModifyingStatement production.
	EnterLinearCatalogModifyingStatement(c *LinearCatalogModifyingStatementContext)

	// EnterSimpleCatalogModifyingStatement is called when entering the simpleCatalogModifyingStatement production.
	EnterSimpleCatalogModifyingStatement(c *SimpleCatalogModifyingStatementContext)

	// EnterPrimitiveCatalogModifyingStatement is called when entering the primitiveCatalogModifyingStatement production.
	EnterPrimitiveCatalogModifyingStatement(c *PrimitiveCatalogModifyingStatementContext)

	// EnterCreateSchemaStatement is called when entering the createSchemaStatement production.
	EnterCreateSchemaStatement(c *CreateSchemaStatementContext)

	// EnterDropSchemaStatement is called when entering the dropSchemaStatement production.
	EnterDropSchemaStatement(c *DropSchemaStatementContext)

	// EnterCreateGraphStatement is called when entering the createGraphStatement production.
	EnterCreateGraphStatement(c *CreateGraphStatementContext)

	// EnterOpenGraphType is called when entering the openGraphType production.
	EnterOpenGraphType(c *OpenGraphTypeContext)

	// EnterOfGraphType is called when entering the ofGraphType production.
	EnterOfGraphType(c *OfGraphTypeContext)

	// EnterGraphTypeLikeGraph is called when entering the graphTypeLikeGraph production.
	EnterGraphTypeLikeGraph(c *GraphTypeLikeGraphContext)

	// EnterGraphSource is called when entering the graphSource production.
	EnterGraphSource(c *GraphSourceContext)

	// EnterDropGraphStatement is called when entering the dropGraphStatement production.
	EnterDropGraphStatement(c *DropGraphStatementContext)

	// EnterCreateGraphTypeStatement is called when entering the createGraphTypeStatement production.
	EnterCreateGraphTypeStatement(c *CreateGraphTypeStatementContext)

	// EnterGraphTypeSource is called when entering the graphTypeSource production.
	EnterGraphTypeSource(c *GraphTypeSourceContext)

	// EnterCopyOfGraphType is called when entering the copyOfGraphType production.
	EnterCopyOfGraphType(c *CopyOfGraphTypeContext)

	// EnterDropGraphTypeStatement is called when entering the dropGraphTypeStatement production.
	EnterDropGraphTypeStatement(c *DropGraphTypeStatementContext)

	// EnterCallCatalogModifyingProcedureStatement is called when entering the callCatalogModifyingProcedureStatement production.
	EnterCallCatalogModifyingProcedureStatement(c *CallCatalogModifyingProcedureStatementContext)

	// EnterLinearDataModifyingStatement is called when entering the linearDataModifyingStatement production.
	EnterLinearDataModifyingStatement(c *LinearDataModifyingStatementContext)

	// EnterFocusedLinearDataModifyingStatement is called when entering the focusedLinearDataModifyingStatement production.
	EnterFocusedLinearDataModifyingStatement(c *FocusedLinearDataModifyingStatementContext)

	// EnterFocusedLinearDataModifyingStatementBody is called when entering the focusedLinearDataModifyingStatementBody production.
	EnterFocusedLinearDataModifyingStatementBody(c *FocusedLinearDataModifyingStatementBodyContext)

	// EnterFocusedNestedDataModifyingProcedureSpecification is called when entering the focusedNestedDataModifyingProcedureSpecification production.
	EnterFocusedNestedDataModifyingProcedureSpecification(c *FocusedNestedDataModifyingProcedureSpecificationContext)

	// EnterAmbientLinearDataModifyingStatement is called when entering the ambientLinearDataModifyingStatement production.
	EnterAmbientLinearDataModifyingStatement(c *AmbientLinearDataModifyingStatementContext)

	// EnterAmbientLinearDataModifyingStatementBody is called when entering the ambientLinearDataModifyingStatementBody production.
	EnterAmbientLinearDataModifyingStatementBody(c *AmbientLinearDataModifyingStatementBodyContext)

	// EnterSimpleLinearDataAccessingStatement is called when entering the simpleLinearDataAccessingStatement production.
	EnterSimpleLinearDataAccessingStatement(c *SimpleLinearDataAccessingStatementContext)

	// EnterSimpleDataAccessingStatement is called when entering the simpleDataAccessingStatement production.
	EnterSimpleDataAccessingStatement(c *SimpleDataAccessingStatementContext)

	// EnterSimpleDataModifyingStatement is called when entering the simpleDataModifyingStatement production.
	EnterSimpleDataModifyingStatement(c *SimpleDataModifyingStatementContext)

	// EnterPrimitiveDataModifyingStatement is called when entering the primitiveDataModifyingStatement production.
	EnterPrimitiveDataModifyingStatement(c *PrimitiveDataModifyingStatementContext)

	// EnterInsertStatement is called when entering the insertStatement production.
	EnterInsertStatement(c *InsertStatementContext)

	// EnterSetStatement is called when entering the setStatement production.
	EnterSetStatement(c *SetStatementContext)

	// EnterSetItemList is called when entering the setItemList production.
	EnterSetItemList(c *SetItemListContext)

	// EnterSetItem is called when entering the setItem production.
	EnterSetItem(c *SetItemContext)

	// EnterSetPropertyItem is called when entering the setPropertyItem production.
	EnterSetPropertyItem(c *SetPropertyItemContext)

	// EnterSetAllPropertiesItem is called when entering the setAllPropertiesItem production.
	EnterSetAllPropertiesItem(c *SetAllPropertiesItemContext)

	// EnterSetLabelItem is called when entering the setLabelItem production.
	EnterSetLabelItem(c *SetLabelItemContext)

	// EnterRemoveStatement is called when entering the removeStatement production.
	EnterRemoveStatement(c *RemoveStatementContext)

	// EnterRemoveItemList is called when entering the removeItemList production.
	EnterRemoveItemList(c *RemoveItemListContext)

	// EnterRemoveItem is called when entering the removeItem production.
	EnterRemoveItem(c *RemoveItemContext)

	// EnterRemovePropertyItem is called when entering the removePropertyItem production.
	EnterRemovePropertyItem(c *RemovePropertyItemContext)

	// EnterRemoveLabelItem is called when entering the removeLabelItem production.
	EnterRemoveLabelItem(c *RemoveLabelItemContext)

	// EnterDeleteStatement is called when entering the deleteStatement production.
	EnterDeleteStatement(c *DeleteStatementContext)

	// EnterDeleteItemList is called when entering the deleteItemList production.
	EnterDeleteItemList(c *DeleteItemListContext)

	// EnterDeleteItem is called when entering the deleteItem production.
	EnterDeleteItem(c *DeleteItemContext)

	// EnterCallDataModifyingProcedureStatement is called when entering the callDataModifyingProcedureStatement production.
	EnterCallDataModifyingProcedureStatement(c *CallDataModifyingProcedureStatementContext)

	// EnterCompositeQueryStatement is called when entering the compositeQueryStatement production.
	EnterCompositeQueryStatement(c *CompositeQueryStatementContext)

	// EnterCompositeQueryExpression is called when entering the compositeQueryExpression production.
	EnterCompositeQueryExpression(c *CompositeQueryExpressionContext)

	// EnterQueryConjunction is called when entering the queryConjunction production.
	EnterQueryConjunction(c *QueryConjunctionContext)

	// EnterSetOperator is called when entering the setOperator production.
	EnterSetOperator(c *SetOperatorContext)

	// EnterCompositeQueryPrimary is called when entering the compositeQueryPrimary production.
	EnterCompositeQueryPrimary(c *CompositeQueryPrimaryContext)

	// EnterLinearQueryStatement is called when entering the linearQueryStatement production.
	EnterLinearQueryStatement(c *LinearQueryStatementContext)

	// EnterFocusedLinearQueryStatement is called when entering the focusedLinearQueryStatement production.
	EnterFocusedLinearQueryStatement(c *FocusedLinearQueryStatementContext)

	// EnterFocusedLinearQueryStatementPart is called when entering the focusedLinearQueryStatementPart production.
	EnterFocusedLinearQueryStatementPart(c *FocusedLinearQueryStatementPartContext)

	// EnterFocusedLinearQueryAndPrimitiveResultStatementPart is called when entering the focusedLinearQueryAndPrimitiveResultStatementPart production.
	EnterFocusedLinearQueryAndPrimitiveResultStatementPart(c *FocusedLinearQueryAndPrimitiveResultStatementPartContext)

	// EnterFocusedPrimitiveResultStatement is called when entering the focusedPrimitiveResultStatement production.
	EnterFocusedPrimitiveResultStatement(c *FocusedPrimitiveResultStatementContext)

	// EnterFocusedNestedQuerySpecification is called when entering the focusedNestedQuerySpecification production.
	EnterFocusedNestedQuerySpecification(c *FocusedNestedQuerySpecificationContext)

	// EnterAmbientLinearQueryStatement is called when entering the ambientLinearQueryStatement production.
	EnterAmbientLinearQueryStatement(c *AmbientLinearQueryStatementContext)

	// EnterSimpleLinearQueryStatement is called when entering the simpleLinearQueryStatement production.
	EnterSimpleLinearQueryStatement(c *SimpleLinearQueryStatementContext)

	// EnterSimpleQueryStatement is called when entering the simpleQueryStatement production.
	EnterSimpleQueryStatement(c *SimpleQueryStatementContext)

	// EnterPrimitiveQueryStatement is called when entering the primitiveQueryStatement production.
	EnterPrimitiveQueryStatement(c *PrimitiveQueryStatementContext)

	// EnterMatchStatement is called when entering the matchStatement production.
	EnterMatchStatement(c *MatchStatementContext)

	// EnterSimpleMatchStatement is called when entering the simpleMatchStatement production.
	EnterSimpleMatchStatement(c *SimpleMatchStatementContext)

	// EnterOptionalMatchStatement is called when entering the optionalMatchStatement production.
	EnterOptionalMatchStatement(c *OptionalMatchStatementContext)

	// EnterOptionalOperand is called when entering the optionalOperand production.
	EnterOptionalOperand(c *OptionalOperandContext)

	// EnterMatchStatementBlock is called when entering the matchStatementBlock production.
	EnterMatchStatementBlock(c *MatchStatementBlockContext)

	// EnterCallQueryStatement is called when entering the callQueryStatement production.
	EnterCallQueryStatement(c *CallQueryStatementContext)

	// EnterFilterStatement is called when entering the filterStatement production.
	EnterFilterStatement(c *FilterStatementContext)

	// EnterLetStatement is called when entering the letStatement production.
	EnterLetStatement(c *LetStatementContext)

	// EnterLetVariableDefinitionList is called when entering the letVariableDefinitionList production.
	EnterLetVariableDefinitionList(c *LetVariableDefinitionListContext)

	// EnterLetVariableDefinition is called when entering the letVariableDefinition production.
	EnterLetVariableDefinition(c *LetVariableDefinitionContext)

	// EnterForStatement is called when entering the forStatement production.
	EnterForStatement(c *ForStatementContext)

	// EnterForItem is called when entering the forItem production.
	EnterForItem(c *ForItemContext)

	// EnterForItemAlias is called when entering the forItemAlias production.
	EnterForItemAlias(c *ForItemAliasContext)

	// EnterForItemSource is called when entering the forItemSource production.
	EnterForItemSource(c *ForItemSourceContext)

	// EnterForOrdinalityOrOffset is called when entering the forOrdinalityOrOffset production.
	EnterForOrdinalityOrOffset(c *ForOrdinalityOrOffsetContext)

	// EnterOrderByAndPageStatement is called when entering the orderByAndPageStatement production.
	EnterOrderByAndPageStatement(c *OrderByAndPageStatementContext)

	// EnterPrimitiveResultStatement is called when entering the primitiveResultStatement production.
	EnterPrimitiveResultStatement(c *PrimitiveResultStatementContext)

	// EnterReturnStatement is called when entering the returnStatement production.
	EnterReturnStatement(c *ReturnStatementContext)

	// EnterReturnStatementBody is called when entering the returnStatementBody production.
	EnterReturnStatementBody(c *ReturnStatementBodyContext)

	// EnterReturnItemList is called when entering the returnItemList production.
	EnterReturnItemList(c *ReturnItemListContext)

	// EnterReturnItem is called when entering the returnItem production.
	EnterReturnItem(c *ReturnItemContext)

	// EnterReturnItemAlias is called when entering the returnItemAlias production.
	EnterReturnItemAlias(c *ReturnItemAliasContext)

	// EnterSelectStatement is called when entering the selectStatement production.
	EnterSelectStatement(c *SelectStatementContext)

	// EnterSelectItemList is called when entering the selectItemList production.
	EnterSelectItemList(c *SelectItemListContext)

	// EnterSelectItem is called when entering the selectItem production.
	EnterSelectItem(c *SelectItemContext)

	// EnterSelectItemAlias is called when entering the selectItemAlias production.
	EnterSelectItemAlias(c *SelectItemAliasContext)

	// EnterHavingClause is called when entering the havingClause production.
	EnterHavingClause(c *HavingClauseContext)

	// EnterSelectStatementBody is called when entering the selectStatementBody production.
	EnterSelectStatementBody(c *SelectStatementBodyContext)

	// EnterSelectGraphMatchList is called when entering the selectGraphMatchList production.
	EnterSelectGraphMatchList(c *SelectGraphMatchListContext)

	// EnterSelectGraphMatch is called when entering the selectGraphMatch production.
	EnterSelectGraphMatch(c *SelectGraphMatchContext)

	// EnterSelectQuerySpecification is called when entering the selectQuerySpecification production.
	EnterSelectQuerySpecification(c *SelectQuerySpecificationContext)

	// EnterCallProcedureStatement is called when entering the callProcedureStatement production.
	EnterCallProcedureStatement(c *CallProcedureStatementContext)

	// EnterProcedureCall is called when entering the procedureCall production.
	EnterProcedureCall(c *ProcedureCallContext)

	// EnterInlineProcedureCall is called when entering the inlineProcedureCall production.
	EnterInlineProcedureCall(c *InlineProcedureCallContext)

	// EnterVariableScopeClause is called when entering the variableScopeClause production.
	EnterVariableScopeClause(c *VariableScopeClauseContext)

	// EnterBindingVariableReferenceList is called when entering the bindingVariableReferenceList production.
	EnterBindingVariableReferenceList(c *BindingVariableReferenceListContext)

	// EnterNamedProcedureCall is called when entering the namedProcedureCall production.
	EnterNamedProcedureCall(c *NamedProcedureCallContext)

	// EnterProcedureArgumentList is called when entering the procedureArgumentList production.
	EnterProcedureArgumentList(c *ProcedureArgumentListContext)

	// EnterProcedureArgument is called when entering the procedureArgument production.
	EnterProcedureArgument(c *ProcedureArgumentContext)

	// EnterAtSchemaClause is called when entering the atSchemaClause production.
	EnterAtSchemaClause(c *AtSchemaClauseContext)

	// EnterUseGraphClause is called when entering the useGraphClause production.
	EnterUseGraphClause(c *UseGraphClauseContext)

	// EnterGraphPatternBindingTable is called when entering the graphPatternBindingTable production.
	EnterGraphPatternBindingTable(c *GraphPatternBindingTableContext)

	// EnterGraphPatternYieldClause is called when entering the graphPatternYieldClause production.
	EnterGraphPatternYieldClause(c *GraphPatternYieldClauseContext)

	// EnterGraphPatternYieldItemList is called when entering the graphPatternYieldItemList production.
	EnterGraphPatternYieldItemList(c *GraphPatternYieldItemListContext)

	// EnterGraphPatternYieldItem is called when entering the graphPatternYieldItem production.
	EnterGraphPatternYieldItem(c *GraphPatternYieldItemContext)

	// EnterGraphPattern is called when entering the graphPattern production.
	EnterGraphPattern(c *GraphPatternContext)

	// EnterMatchMode is called when entering the matchMode production.
	EnterMatchMode(c *MatchModeContext)

	// EnterRepeatableElementsMatchMode is called when entering the repeatableElementsMatchMode production.
	EnterRepeatableElementsMatchMode(c *RepeatableElementsMatchModeContext)

	// EnterDifferentEdgesMatchMode is called when entering the differentEdgesMatchMode production.
	EnterDifferentEdgesMatchMode(c *DifferentEdgesMatchModeContext)

	// EnterElementBindingsOrElements is called when entering the elementBindingsOrElements production.
	EnterElementBindingsOrElements(c *ElementBindingsOrElementsContext)

	// EnterEdgeBindingsOrEdges is called when entering the edgeBindingsOrEdges production.
	EnterEdgeBindingsOrEdges(c *EdgeBindingsOrEdgesContext)

	// EnterPathPatternList is called when entering the pathPatternList production.
	EnterPathPatternList(c *PathPatternListContext)

	// EnterPathPattern is called when entering the pathPattern production.
	EnterPathPattern(c *PathPatternContext)

	// EnterPathVariableDeclaration is called when entering the pathVariableDeclaration production.
	EnterPathVariableDeclaration(c *PathVariableDeclarationContext)

	// EnterKeepClause is called when entering the keepClause production.
	EnterKeepClause(c *KeepClauseContext)

	// EnterGraphPatternWhereClause is called when entering the graphPatternWhereClause production.
	EnterGraphPatternWhereClause(c *GraphPatternWhereClauseContext)

	// EnterInsertGraphPattern is called when entering the insertGraphPattern production.
	EnterInsertGraphPattern(c *InsertGraphPatternContext)

	// EnterInsertPathPatternList is called when entering the insertPathPatternList production.
	EnterInsertPathPatternList(c *InsertPathPatternListContext)

	// EnterInsertPathPattern is called when entering the insertPathPattern production.
	EnterInsertPathPattern(c *InsertPathPatternContext)

	// EnterInsertNodePattern is called when entering the insertNodePattern production.
	EnterInsertNodePattern(c *InsertNodePatternContext)

	// EnterInsertEdgePattern is called when entering the insertEdgePattern production.
	EnterInsertEdgePattern(c *InsertEdgePatternContext)

	// EnterInsertEdgePointingLeft is called when entering the insertEdgePointingLeft production.
	EnterInsertEdgePointingLeft(c *InsertEdgePointingLeftContext)

	// EnterInsertEdgePointingRight is called when entering the insertEdgePointingRight production.
	EnterInsertEdgePointingRight(c *InsertEdgePointingRightContext)

	// EnterInsertEdgeUndirected is called when entering the insertEdgeUndirected production.
	EnterInsertEdgeUndirected(c *InsertEdgeUndirectedContext)

	// EnterInsertElementPatternFiller is called when entering the insertElementPatternFiller production.
	EnterInsertElementPatternFiller(c *InsertElementPatternFillerContext)

	// EnterLabelAndPropertySetSpecification is called when entering the labelAndPropertySetSpecification production.
	EnterLabelAndPropertySetSpecification(c *LabelAndPropertySetSpecificationContext)

	// EnterPathPatternPrefix is called when entering the pathPatternPrefix production.
	EnterPathPatternPrefix(c *PathPatternPrefixContext)

	// EnterPathModePrefix is called when entering the pathModePrefix production.
	EnterPathModePrefix(c *PathModePrefixContext)

	// EnterPathMode is called when entering the pathMode production.
	EnterPathMode(c *PathModeContext)

	// EnterPathSearchPrefix is called when entering the pathSearchPrefix production.
	EnterPathSearchPrefix(c *PathSearchPrefixContext)

	// EnterAllPathSearch is called when entering the allPathSearch production.
	EnterAllPathSearch(c *AllPathSearchContext)

	// EnterPathOrPaths is called when entering the pathOrPaths production.
	EnterPathOrPaths(c *PathOrPathsContext)

	// EnterAnyPathSearch is called when entering the anyPathSearch production.
	EnterAnyPathSearch(c *AnyPathSearchContext)

	// EnterNumberOfPaths is called when entering the numberOfPaths production.
	EnterNumberOfPaths(c *NumberOfPathsContext)

	// EnterShortestPathSearch is called when entering the shortestPathSearch production.
	EnterShortestPathSearch(c *ShortestPathSearchContext)

	// EnterAllShortestPathSearch is called when entering the allShortestPathSearch production.
	EnterAllShortestPathSearch(c *AllShortestPathSearchContext)

	// EnterAnyShortestPathSearch is called when entering the anyShortestPathSearch production.
	EnterAnyShortestPathSearch(c *AnyShortestPathSearchContext)

	// EnterCountedShortestPathSearch is called when entering the countedShortestPathSearch production.
	EnterCountedShortestPathSearch(c *CountedShortestPathSearchContext)

	// EnterCountedShortestGroupSearch is called when entering the countedShortestGroupSearch production.
	EnterCountedShortestGroupSearch(c *CountedShortestGroupSearchContext)

	// EnterNumberOfGroups is called when entering the numberOfGroups production.
	EnterNumberOfGroups(c *NumberOfGroupsContext)

	// EnterPpePathTerm is called when entering the ppePathTerm production.
	EnterPpePathTerm(c *PpePathTermContext)

	// EnterPpeMultisetAlternation is called when entering the ppeMultisetAlternation production.
	EnterPpeMultisetAlternation(c *PpeMultisetAlternationContext)

	// EnterPpePatternUnion is called when entering the ppePatternUnion production.
	EnterPpePatternUnion(c *PpePatternUnionContext)

	// EnterPathTerm is called when entering the pathTerm production.
	EnterPathTerm(c *PathTermContext)

	// EnterPfPathPrimary is called when entering the pfPathPrimary production.
	EnterPfPathPrimary(c *PfPathPrimaryContext)

	// EnterPfQuantifiedPathPrimary is called when entering the pfQuantifiedPathPrimary production.
	EnterPfQuantifiedPathPrimary(c *PfQuantifiedPathPrimaryContext)

	// EnterPfQuestionedPathPrimary is called when entering the pfQuestionedPathPrimary production.
	EnterPfQuestionedPathPrimary(c *PfQuestionedPathPrimaryContext)

	// EnterPpElementPattern is called when entering the ppElementPattern production.
	EnterPpElementPattern(c *PpElementPatternContext)

	// EnterPpParenthesizedPathPatternExpression is called when entering the ppParenthesizedPathPatternExpression production.
	EnterPpParenthesizedPathPatternExpression(c *PpParenthesizedPathPatternExpressionContext)

	// EnterPpSimplifiedPathPatternExpression is called when entering the ppSimplifiedPathPatternExpression production.
	EnterPpSimplifiedPathPatternExpression(c *PpSimplifiedPathPatternExpressionContext)

	// EnterElementPattern is called when entering the elementPattern production.
	EnterElementPattern(c *ElementPatternContext)

	// EnterNodePattern is called when entering the nodePattern production.
	EnterNodePattern(c *NodePatternContext)

	// EnterElementPatternFiller is called when entering the elementPatternFiller production.
	EnterElementPatternFiller(c *ElementPatternFillerContext)

	// EnterElementVariableDeclaration is called when entering the elementVariableDeclaration production.
	EnterElementVariableDeclaration(c *ElementVariableDeclarationContext)

	// EnterIsLabelExpression is called when entering the isLabelExpression production.
	EnterIsLabelExpression(c *IsLabelExpressionContext)

	// EnterIsOrColon is called when entering the isOrColon production.
	EnterIsOrColon(c *IsOrColonContext)

	// EnterElementPatternPredicate is called when entering the elementPatternPredicate production.
	EnterElementPatternPredicate(c *ElementPatternPredicateContext)

	// EnterElementPatternWhereClause is called when entering the elementPatternWhereClause production.
	EnterElementPatternWhereClause(c *ElementPatternWhereClauseContext)

	// EnterElementPropertySpecification is called when entering the elementPropertySpecification production.
	EnterElementPropertySpecification(c *ElementPropertySpecificationContext)

	// EnterPropertyKeyValuePairList is called when entering the propertyKeyValuePairList production.
	EnterPropertyKeyValuePairList(c *PropertyKeyValuePairListContext)

	// EnterPropertyKeyValuePair is called when entering the propertyKeyValuePair production.
	EnterPropertyKeyValuePair(c *PropertyKeyValuePairContext)

	// EnterEdgePattern is called when entering the edgePattern production.
	EnterEdgePattern(c *EdgePatternContext)

	// EnterFullEdgePattern is called when entering the fullEdgePattern production.
	EnterFullEdgePattern(c *FullEdgePatternContext)

	// EnterFullEdgePointingLeft is called when entering the fullEdgePointingLeft production.
	EnterFullEdgePointingLeft(c *FullEdgePointingLeftContext)

	// EnterFullEdgeUndirected is called when entering the fullEdgeUndirected production.
	EnterFullEdgeUndirected(c *FullEdgeUndirectedContext)

	// EnterFullEdgePointingRight is called when entering the fullEdgePointingRight production.
	EnterFullEdgePointingRight(c *FullEdgePointingRightContext)

	// EnterFullEdgeLeftOrUndirected is called when entering the fullEdgeLeftOrUndirected production.
	EnterFullEdgeLeftOrUndirected(c *FullEdgeLeftOrUndirectedContext)

	// EnterFullEdgeUndirectedOrRight is called when entering the fullEdgeUndirectedOrRight production.
	EnterFullEdgeUndirectedOrRight(c *FullEdgeUndirectedOrRightContext)

	// EnterFullEdgeLeftOrRight is called when entering the fullEdgeLeftOrRight production.
	EnterFullEdgeLeftOrRight(c *FullEdgeLeftOrRightContext)

	// EnterFullEdgeAnyDirection is called when entering the fullEdgeAnyDirection production.
	EnterFullEdgeAnyDirection(c *FullEdgeAnyDirectionContext)

	// EnterAbbreviatedEdgePattern is called when entering the abbreviatedEdgePattern production.
	EnterAbbreviatedEdgePattern(c *AbbreviatedEdgePatternContext)

	// EnterParenthesizedPathPatternExpression is called when entering the parenthesizedPathPatternExpression production.
	EnterParenthesizedPathPatternExpression(c *ParenthesizedPathPatternExpressionContext)

	// EnterSubpathVariableDeclaration is called when entering the subpathVariableDeclaration production.
	EnterSubpathVariableDeclaration(c *SubpathVariableDeclarationContext)

	// EnterParenthesizedPathPatternWhereClause is called when entering the parenthesizedPathPatternWhereClause production.
	EnterParenthesizedPathPatternWhereClause(c *ParenthesizedPathPatternWhereClauseContext)

	// EnterLabelExpressionNegation is called when entering the labelExpressionNegation production.
	EnterLabelExpressionNegation(c *LabelExpressionNegationContext)

	// EnterLabelExpressionDisjunction is called when entering the labelExpressionDisjunction production.
	EnterLabelExpressionDisjunction(c *LabelExpressionDisjunctionContext)

	// EnterLabelExpressionParenthesized is called when entering the labelExpressionParenthesized production.
	EnterLabelExpressionParenthesized(c *LabelExpressionParenthesizedContext)

	// EnterLabelExpressionWildcard is called when entering the labelExpressionWildcard production.
	EnterLabelExpressionWildcard(c *LabelExpressionWildcardContext)

	// EnterLabelExpressionConjunction is called when entering the labelExpressionConjunction production.
	EnterLabelExpressionConjunction(c *LabelExpressionConjunctionContext)

	// EnterLabelExpressionName is called when entering the labelExpressionName production.
	EnterLabelExpressionName(c *LabelExpressionNameContext)

	// EnterPathVariableReference is called when entering the pathVariableReference production.
	EnterPathVariableReference(c *PathVariableReferenceContext)

	// EnterElementVariableReference is called when entering the elementVariableReference production.
	EnterElementVariableReference(c *ElementVariableReferenceContext)

	// EnterGraphPatternQuantifier is called when entering the graphPatternQuantifier production.
	EnterGraphPatternQuantifier(c *GraphPatternQuantifierContext)

	// EnterFixedQuantifier is called when entering the fixedQuantifier production.
	EnterFixedQuantifier(c *FixedQuantifierContext)

	// EnterGeneralQuantifier is called when entering the generalQuantifier production.
	EnterGeneralQuantifier(c *GeneralQuantifierContext)

	// EnterLowerBound is called when entering the lowerBound production.
	EnterLowerBound(c *LowerBoundContext)

	// EnterUpperBound is called when entering the upperBound production.
	EnterUpperBound(c *UpperBoundContext)

	// EnterSimplifiedPathPatternExpression is called when entering the simplifiedPathPatternExpression production.
	EnterSimplifiedPathPatternExpression(c *SimplifiedPathPatternExpressionContext)

	// EnterSimplifiedDefaultingLeft is called when entering the simplifiedDefaultingLeft production.
	EnterSimplifiedDefaultingLeft(c *SimplifiedDefaultingLeftContext)

	// EnterSimplifiedDefaultingUndirected is called when entering the simplifiedDefaultingUndirected production.
	EnterSimplifiedDefaultingUndirected(c *SimplifiedDefaultingUndirectedContext)

	// EnterSimplifiedDefaultingRight is called when entering the simplifiedDefaultingRight production.
	EnterSimplifiedDefaultingRight(c *SimplifiedDefaultingRightContext)

	// EnterSimplifiedDefaultingLeftOrUndirected is called when entering the simplifiedDefaultingLeftOrUndirected production.
	EnterSimplifiedDefaultingLeftOrUndirected(c *SimplifiedDefaultingLeftOrUndirectedContext)

	// EnterSimplifiedDefaultingUndirectedOrRight is called when entering the simplifiedDefaultingUndirectedOrRight production.
	EnterSimplifiedDefaultingUndirectedOrRight(c *SimplifiedDefaultingUndirectedOrRightContext)

	// EnterSimplifiedDefaultingLeftOrRight is called when entering the simplifiedDefaultingLeftOrRight production.
	EnterSimplifiedDefaultingLeftOrRight(c *SimplifiedDefaultingLeftOrRightContext)

	// EnterSimplifiedDefaultingAnyDirection is called when entering the simplifiedDefaultingAnyDirection production.
	EnterSimplifiedDefaultingAnyDirection(c *SimplifiedDefaultingAnyDirectionContext)

	// EnterSimplifiedContents is called when entering the simplifiedContents production.
	EnterSimplifiedContents(c *SimplifiedContentsContext)

	// EnterSimplifiedPathUnion is called when entering the simplifiedPathUnion production.
	EnterSimplifiedPathUnion(c *SimplifiedPathUnionContext)

	// EnterSimplifiedMultisetAlternation is called when entering the simplifiedMultisetAlternation production.
	EnterSimplifiedMultisetAlternation(c *SimplifiedMultisetAlternationContext)

	// EnterSimplifiedFactorLowLabel is called when entering the simplifiedFactorLowLabel production.
	EnterSimplifiedFactorLowLabel(c *SimplifiedFactorLowLabelContext)

	// EnterSimplifiedConcatenationLabel is called when entering the simplifiedConcatenationLabel production.
	EnterSimplifiedConcatenationLabel(c *SimplifiedConcatenationLabelContext)

	// EnterSimplifiedConjunctionLabel is called when entering the simplifiedConjunctionLabel production.
	EnterSimplifiedConjunctionLabel(c *SimplifiedConjunctionLabelContext)

	// EnterSimplifiedFactorHighLabel is called when entering the simplifiedFactorHighLabel production.
	EnterSimplifiedFactorHighLabel(c *SimplifiedFactorHighLabelContext)

	// EnterSimplifiedFactorHigh is called when entering the simplifiedFactorHigh production.
	EnterSimplifiedFactorHigh(c *SimplifiedFactorHighContext)

	// EnterSimplifiedQuantified is called when entering the simplifiedQuantified production.
	EnterSimplifiedQuantified(c *SimplifiedQuantifiedContext)

	// EnterSimplifiedQuestioned is called when entering the simplifiedQuestioned production.
	EnterSimplifiedQuestioned(c *SimplifiedQuestionedContext)

	// EnterSimplifiedTertiary is called when entering the simplifiedTertiary production.
	EnterSimplifiedTertiary(c *SimplifiedTertiaryContext)

	// EnterSimplifiedDirectionOverride is called when entering the simplifiedDirectionOverride production.
	EnterSimplifiedDirectionOverride(c *SimplifiedDirectionOverrideContext)

	// EnterSimplifiedOverrideLeft is called when entering the simplifiedOverrideLeft production.
	EnterSimplifiedOverrideLeft(c *SimplifiedOverrideLeftContext)

	// EnterSimplifiedOverrideUndirected is called when entering the simplifiedOverrideUndirected production.
	EnterSimplifiedOverrideUndirected(c *SimplifiedOverrideUndirectedContext)

	// EnterSimplifiedOverrideRight is called when entering the simplifiedOverrideRight production.
	EnterSimplifiedOverrideRight(c *SimplifiedOverrideRightContext)

	// EnterSimplifiedOverrideLeftOrUndirected is called when entering the simplifiedOverrideLeftOrUndirected production.
	EnterSimplifiedOverrideLeftOrUndirected(c *SimplifiedOverrideLeftOrUndirectedContext)

	// EnterSimplifiedOverrideUndirectedOrRight is called when entering the simplifiedOverrideUndirectedOrRight production.
	EnterSimplifiedOverrideUndirectedOrRight(c *SimplifiedOverrideUndirectedOrRightContext)

	// EnterSimplifiedOverrideLeftOrRight is called when entering the simplifiedOverrideLeftOrRight production.
	EnterSimplifiedOverrideLeftOrRight(c *SimplifiedOverrideLeftOrRightContext)

	// EnterSimplifiedOverrideAnyDirection is called when entering the simplifiedOverrideAnyDirection production.
	EnterSimplifiedOverrideAnyDirection(c *SimplifiedOverrideAnyDirectionContext)

	// EnterSimplifiedSecondary is called when entering the simplifiedSecondary production.
	EnterSimplifiedSecondary(c *SimplifiedSecondaryContext)

	// EnterSimplifiedNegation is called when entering the simplifiedNegation production.
	EnterSimplifiedNegation(c *SimplifiedNegationContext)

	// EnterSimplifiedPrimary is called when entering the simplifiedPrimary production.
	EnterSimplifiedPrimary(c *SimplifiedPrimaryContext)

	// EnterWhereClause is called when entering the whereClause production.
	EnterWhereClause(c *WhereClauseContext)

	// EnterYieldClause is called when entering the yieldClause production.
	EnterYieldClause(c *YieldClauseContext)

	// EnterYieldItemList is called when entering the yieldItemList production.
	EnterYieldItemList(c *YieldItemListContext)

	// EnterYieldItem is called when entering the yieldItem production.
	EnterYieldItem(c *YieldItemContext)

	// EnterYieldItemName is called when entering the yieldItemName production.
	EnterYieldItemName(c *YieldItemNameContext)

	// EnterYieldItemAlias is called when entering the yieldItemAlias production.
	EnterYieldItemAlias(c *YieldItemAliasContext)

	// EnterGroupByClause is called when entering the groupByClause production.
	EnterGroupByClause(c *GroupByClauseContext)

	// EnterGroupingElementList is called when entering the groupingElementList production.
	EnterGroupingElementList(c *GroupingElementListContext)

	// EnterGroupingElement is called when entering the groupingElement production.
	EnterGroupingElement(c *GroupingElementContext)

	// EnterEmptyGroupingSet is called when entering the emptyGroupingSet production.
	EnterEmptyGroupingSet(c *EmptyGroupingSetContext)

	// EnterOrderByClause is called when entering the orderByClause production.
	EnterOrderByClause(c *OrderByClauseContext)

	// EnterSortSpecificationList is called when entering the sortSpecificationList production.
	EnterSortSpecificationList(c *SortSpecificationListContext)

	// EnterSortSpecification is called when entering the sortSpecification production.
	EnterSortSpecification(c *SortSpecificationContext)

	// EnterSortKey is called when entering the sortKey production.
	EnterSortKey(c *SortKeyContext)

	// EnterOrderingSpecification is called when entering the orderingSpecification production.
	EnterOrderingSpecification(c *OrderingSpecificationContext)

	// EnterNullOrdering is called when entering the nullOrdering production.
	EnterNullOrdering(c *NullOrderingContext)

	// EnterLimitClause is called when entering the limitClause production.
	EnterLimitClause(c *LimitClauseContext)

	// EnterOffsetClause is called when entering the offsetClause production.
	EnterOffsetClause(c *OffsetClauseContext)

	// EnterOffsetSynonym is called when entering the offsetSynonym production.
	EnterOffsetSynonym(c *OffsetSynonymContext)

	// EnterSchemaReference is called when entering the schemaReference production.
	EnterSchemaReference(c *SchemaReferenceContext)

	// EnterAbsoluteCatalogSchemaReference is called when entering the absoluteCatalogSchemaReference production.
	EnterAbsoluteCatalogSchemaReference(c *AbsoluteCatalogSchemaReferenceContext)

	// EnterCatalogSchemaParentAndName is called when entering the catalogSchemaParentAndName production.
	EnterCatalogSchemaParentAndName(c *CatalogSchemaParentAndNameContext)

	// EnterRelativeCatalogSchemaReference is called when entering the relativeCatalogSchemaReference production.
	EnterRelativeCatalogSchemaReference(c *RelativeCatalogSchemaReferenceContext)

	// EnterPredefinedSchemaReference is called when entering the predefinedSchemaReference production.
	EnterPredefinedSchemaReference(c *PredefinedSchemaReferenceContext)

	// EnterAbsoluteDirectoryPath is called when entering the absoluteDirectoryPath production.
	EnterAbsoluteDirectoryPath(c *AbsoluteDirectoryPathContext)

	// EnterRelativeDirectoryPath is called when entering the relativeDirectoryPath production.
	EnterRelativeDirectoryPath(c *RelativeDirectoryPathContext)

	// EnterSimpleDirectoryPath is called when entering the simpleDirectoryPath production.
	EnterSimpleDirectoryPath(c *SimpleDirectoryPathContext)

	// EnterGraphReference is called when entering the graphReference production.
	EnterGraphReference(c *GraphReferenceContext)

	// EnterCatalogGraphParentAndName is called when entering the catalogGraphParentAndName production.
	EnterCatalogGraphParentAndName(c *CatalogGraphParentAndNameContext)

	// EnterHomeGraph is called when entering the homeGraph production.
	EnterHomeGraph(c *HomeGraphContext)

	// EnterGraphTypeReference is called when entering the graphTypeReference production.
	EnterGraphTypeReference(c *GraphTypeReferenceContext)

	// EnterCatalogGraphTypeParentAndName is called when entering the catalogGraphTypeParentAndName production.
	EnterCatalogGraphTypeParentAndName(c *CatalogGraphTypeParentAndNameContext)

	// EnterBindingTableReference is called when entering the bindingTableReference production.
	EnterBindingTableReference(c *BindingTableReferenceContext)

	// EnterProcedureReference is called when entering the procedureReference production.
	EnterProcedureReference(c *ProcedureReferenceContext)

	// EnterCatalogProcedureParentAndName is called when entering the catalogProcedureParentAndName production.
	EnterCatalogProcedureParentAndName(c *CatalogProcedureParentAndNameContext)

	// EnterCatalogObjectParentReference is called when entering the catalogObjectParentReference production.
	EnterCatalogObjectParentReference(c *CatalogObjectParentReferenceContext)

	// EnterReferenceParameterSpecification is called when entering the referenceParameterSpecification production.
	EnterReferenceParameterSpecification(c *ReferenceParameterSpecificationContext)

	// EnterNestedGraphTypeSpecification is called when entering the nestedGraphTypeSpecification production.
	EnterNestedGraphTypeSpecification(c *NestedGraphTypeSpecificationContext)

	// EnterGraphTypeSpecificationBody is called when entering the graphTypeSpecificationBody production.
	EnterGraphTypeSpecificationBody(c *GraphTypeSpecificationBodyContext)

	// EnterElementTypeList is called when entering the elementTypeList production.
	EnterElementTypeList(c *ElementTypeListContext)

	// EnterElementTypeSpecification is called when entering the elementTypeSpecification production.
	EnterElementTypeSpecification(c *ElementTypeSpecificationContext)

	// EnterNodeTypeSpecification is called when entering the nodeTypeSpecification production.
	EnterNodeTypeSpecification(c *NodeTypeSpecificationContext)

	// EnterNodeTypePattern is called when entering the nodeTypePattern production.
	EnterNodeTypePattern(c *NodeTypePatternContext)

	// EnterNodeTypePhrase is called when entering the nodeTypePhrase production.
	EnterNodeTypePhrase(c *NodeTypePhraseContext)

	// EnterNodeTypePhraseFiller is called when entering the nodeTypePhraseFiller production.
	EnterNodeTypePhraseFiller(c *NodeTypePhraseFillerContext)

	// EnterNodeTypeFiller is called when entering the nodeTypeFiller production.
	EnterNodeTypeFiller(c *NodeTypeFillerContext)

	// EnterLocalNodeTypeAlias is called when entering the localNodeTypeAlias production.
	EnterLocalNodeTypeAlias(c *LocalNodeTypeAliasContext)

	// EnterNodeTypeImpliedContent is called when entering the nodeTypeImpliedContent production.
	EnterNodeTypeImpliedContent(c *NodeTypeImpliedContentContext)

	// EnterNodeTypeKeyLabelSet is called when entering the nodeTypeKeyLabelSet production.
	EnterNodeTypeKeyLabelSet(c *NodeTypeKeyLabelSetContext)

	// EnterNodeTypeLabelSet is called when entering the nodeTypeLabelSet production.
	EnterNodeTypeLabelSet(c *NodeTypeLabelSetContext)

	// EnterNodeTypePropertyTypes is called when entering the nodeTypePropertyTypes production.
	EnterNodeTypePropertyTypes(c *NodeTypePropertyTypesContext)

	// EnterEdgeTypeSpecification is called when entering the edgeTypeSpecification production.
	EnterEdgeTypeSpecification(c *EdgeTypeSpecificationContext)

	// EnterEdgeTypePattern is called when entering the edgeTypePattern production.
	EnterEdgeTypePattern(c *EdgeTypePatternContext)

	// EnterEdgeTypePhrase is called when entering the edgeTypePhrase production.
	EnterEdgeTypePhrase(c *EdgeTypePhraseContext)

	// EnterEdgeTypePhraseFiller is called when entering the edgeTypePhraseFiller production.
	EnterEdgeTypePhraseFiller(c *EdgeTypePhraseFillerContext)

	// EnterEdgeTypeFiller is called when entering the edgeTypeFiller production.
	EnterEdgeTypeFiller(c *EdgeTypeFillerContext)

	// EnterEdgeTypeImpliedContent is called when entering the edgeTypeImpliedContent production.
	EnterEdgeTypeImpliedContent(c *EdgeTypeImpliedContentContext)

	// EnterEdgeTypeKeyLabelSet is called when entering the edgeTypeKeyLabelSet production.
	EnterEdgeTypeKeyLabelSet(c *EdgeTypeKeyLabelSetContext)

	// EnterEdgeTypeLabelSet is called when entering the edgeTypeLabelSet production.
	EnterEdgeTypeLabelSet(c *EdgeTypeLabelSetContext)

	// EnterEdgeTypePropertyTypes is called when entering the edgeTypePropertyTypes production.
	EnterEdgeTypePropertyTypes(c *EdgeTypePropertyTypesContext)

	// EnterEdgeTypePatternDirected is called when entering the edgeTypePatternDirected production.
	EnterEdgeTypePatternDirected(c *EdgeTypePatternDirectedContext)

	// EnterEdgeTypePatternPointingRight is called when entering the edgeTypePatternPointingRight production.
	EnterEdgeTypePatternPointingRight(c *EdgeTypePatternPointingRightContext)

	// EnterEdgeTypePatternPointingLeft is called when entering the edgeTypePatternPointingLeft production.
	EnterEdgeTypePatternPointingLeft(c *EdgeTypePatternPointingLeftContext)

	// EnterEdgeTypePatternUndirected is called when entering the edgeTypePatternUndirected production.
	EnterEdgeTypePatternUndirected(c *EdgeTypePatternUndirectedContext)

	// EnterArcTypePointingRight is called when entering the arcTypePointingRight production.
	EnterArcTypePointingRight(c *ArcTypePointingRightContext)

	// EnterArcTypePointingLeft is called when entering the arcTypePointingLeft production.
	EnterArcTypePointingLeft(c *ArcTypePointingLeftContext)

	// EnterArcTypeUndirected is called when entering the arcTypeUndirected production.
	EnterArcTypeUndirected(c *ArcTypeUndirectedContext)

	// EnterSourceNodeTypeReference is called when entering the sourceNodeTypeReference production.
	EnterSourceNodeTypeReference(c *SourceNodeTypeReferenceContext)

	// EnterDestinationNodeTypeReference is called when entering the destinationNodeTypeReference production.
	EnterDestinationNodeTypeReference(c *DestinationNodeTypeReferenceContext)

	// EnterEdgeKind is called when entering the edgeKind production.
	EnterEdgeKind(c *EdgeKindContext)

	// EnterEndpointPairPhrase is called when entering the endpointPairPhrase production.
	EnterEndpointPairPhrase(c *EndpointPairPhraseContext)

	// EnterEndpointPair is called when entering the endpointPair production.
	EnterEndpointPair(c *EndpointPairContext)

	// EnterEndpointPairDirected is called when entering the endpointPairDirected production.
	EnterEndpointPairDirected(c *EndpointPairDirectedContext)

	// EnterEndpointPairPointingRight is called when entering the endpointPairPointingRight production.
	EnterEndpointPairPointingRight(c *EndpointPairPointingRightContext)

	// EnterEndpointPairPointingLeft is called when entering the endpointPairPointingLeft production.
	EnterEndpointPairPointingLeft(c *EndpointPairPointingLeftContext)

	// EnterEndpointPairUndirected is called when entering the endpointPairUndirected production.
	EnterEndpointPairUndirected(c *EndpointPairUndirectedContext)

	// EnterConnectorPointingRight is called when entering the connectorPointingRight production.
	EnterConnectorPointingRight(c *ConnectorPointingRightContext)

	// EnterConnectorUndirected is called when entering the connectorUndirected production.
	EnterConnectorUndirected(c *ConnectorUndirectedContext)

	// EnterSourceNodeTypeAlias is called when entering the sourceNodeTypeAlias production.
	EnterSourceNodeTypeAlias(c *SourceNodeTypeAliasContext)

	// EnterDestinationNodeTypeAlias is called when entering the destinationNodeTypeAlias production.
	EnterDestinationNodeTypeAlias(c *DestinationNodeTypeAliasContext)

	// EnterLabelSetPhrase is called when entering the labelSetPhrase production.
	EnterLabelSetPhrase(c *LabelSetPhraseContext)

	// EnterLabelSetSpecification is called when entering the labelSetSpecification production.
	EnterLabelSetSpecification(c *LabelSetSpecificationContext)

	// EnterPropertyTypesSpecification is called when entering the propertyTypesSpecification production.
	EnterPropertyTypesSpecification(c *PropertyTypesSpecificationContext)

	// EnterPropertyTypeList is called when entering the propertyTypeList production.
	EnterPropertyTypeList(c *PropertyTypeListContext)

	// EnterPropertyType is called when entering the propertyType production.
	EnterPropertyType(c *PropertyTypeContext)

	// EnterPropertyValueType is called when entering the propertyValueType production.
	EnterPropertyValueType(c *PropertyValueTypeContext)

	// EnterBindingTableType is called when entering the bindingTableType production.
	EnterBindingTableType(c *BindingTableTypeContext)

	// EnterDynamicPropertyValueTypeLabel is called when entering the dynamicPropertyValueTypeLabel production.
	EnterDynamicPropertyValueTypeLabel(c *DynamicPropertyValueTypeLabelContext)

	// EnterClosedDynamicUnionTypeAtl1 is called when entering the closedDynamicUnionTypeAtl1 production.
	EnterClosedDynamicUnionTypeAtl1(c *ClosedDynamicUnionTypeAtl1Context)

	// EnterClosedDynamicUnionTypeAtl2 is called when entering the closedDynamicUnionTypeAtl2 production.
	EnterClosedDynamicUnionTypeAtl2(c *ClosedDynamicUnionTypeAtl2Context)

	// EnterPathValueTypeLabel is called when entering the pathValueTypeLabel production.
	EnterPathValueTypeLabel(c *PathValueTypeLabelContext)

	// EnterListValueTypeAlt3 is called when entering the listValueTypeAlt3 production.
	EnterListValueTypeAlt3(c *ListValueTypeAlt3Context)

	// EnterListValueTypeAlt2 is called when entering the listValueTypeAlt2 production.
	EnterListValueTypeAlt2(c *ListValueTypeAlt2Context)

	// EnterListValueTypeAlt1 is called when entering the listValueTypeAlt1 production.
	EnterListValueTypeAlt1(c *ListValueTypeAlt1Context)

	// EnterPredefinedTypeLabel is called when entering the predefinedTypeLabel production.
	EnterPredefinedTypeLabel(c *PredefinedTypeLabelContext)

	// EnterRecordTypeLabel is called when entering the recordTypeLabel production.
	EnterRecordTypeLabel(c *RecordTypeLabelContext)

	// EnterOpenDynamicUnionTypeLabel is called when entering the openDynamicUnionTypeLabel production.
	EnterOpenDynamicUnionTypeLabel(c *OpenDynamicUnionTypeLabelContext)

	// EnterTyped is called when entering the typed production.
	EnterTyped(c *TypedContext)

	// EnterPredefinedType is called when entering the predefinedType production.
	EnterPredefinedType(c *PredefinedTypeContext)

	// EnterBooleanType is called when entering the booleanType production.
	EnterBooleanType(c *BooleanTypeContext)

	// EnterCharacterStringType is called when entering the characterStringType production.
	EnterCharacterStringType(c *CharacterStringTypeContext)

	// EnterByteStringType is called when entering the byteStringType production.
	EnterByteStringType(c *ByteStringTypeContext)

	// EnterMinLength is called when entering the minLength production.
	EnterMinLength(c *MinLengthContext)

	// EnterMaxLength is called when entering the maxLength production.
	EnterMaxLength(c *MaxLengthContext)

	// EnterFixedLength is called when entering the fixedLength production.
	EnterFixedLength(c *FixedLengthContext)

	// EnterNumericType is called when entering the numericType production.
	EnterNumericType(c *NumericTypeContext)

	// EnterExactNumericType is called when entering the exactNumericType production.
	EnterExactNumericType(c *ExactNumericTypeContext)

	// EnterBinaryExactNumericType is called when entering the binaryExactNumericType production.
	EnterBinaryExactNumericType(c *BinaryExactNumericTypeContext)

	// EnterSignedBinaryExactNumericType is called when entering the signedBinaryExactNumericType production.
	EnterSignedBinaryExactNumericType(c *SignedBinaryExactNumericTypeContext)

	// EnterUnsignedBinaryExactNumericType is called when entering the unsignedBinaryExactNumericType production.
	EnterUnsignedBinaryExactNumericType(c *UnsignedBinaryExactNumericTypeContext)

	// EnterVerboseBinaryExactNumericType is called when entering the verboseBinaryExactNumericType production.
	EnterVerboseBinaryExactNumericType(c *VerboseBinaryExactNumericTypeContext)

	// EnterDecimalExactNumericType is called when entering the decimalExactNumericType production.
	EnterDecimalExactNumericType(c *DecimalExactNumericTypeContext)

	// EnterPrecision is called when entering the precision production.
	EnterPrecision(c *PrecisionContext)

	// EnterScale is called when entering the scale production.
	EnterScale(c *ScaleContext)

	// EnterApproximateNumericType is called when entering the approximateNumericType production.
	EnterApproximateNumericType(c *ApproximateNumericTypeContext)

	// EnterTemporalType is called when entering the temporalType production.
	EnterTemporalType(c *TemporalTypeContext)

	// EnterTemporalInstantType is called when entering the temporalInstantType production.
	EnterTemporalInstantType(c *TemporalInstantTypeContext)

	// EnterDatetimeType is called when entering the datetimeType production.
	EnterDatetimeType(c *DatetimeTypeContext)

	// EnterLocaldatetimeType is called when entering the localdatetimeType production.
	EnterLocaldatetimeType(c *LocaldatetimeTypeContext)

	// EnterDateType is called when entering the dateType production.
	EnterDateType(c *DateTypeContext)

	// EnterTimeType is called when entering the timeType production.
	EnterTimeType(c *TimeTypeContext)

	// EnterLocaltimeType is called when entering the localtimeType production.
	EnterLocaltimeType(c *LocaltimeTypeContext)

	// EnterTemporalDurationType is called when entering the temporalDurationType production.
	EnterTemporalDurationType(c *TemporalDurationTypeContext)

	// EnterTemporalDurationQualifier is called when entering the temporalDurationQualifier production.
	EnterTemporalDurationQualifier(c *TemporalDurationQualifierContext)

	// EnterReferenceValueType is called when entering the referenceValueType production.
	EnterReferenceValueType(c *ReferenceValueTypeContext)

	// EnterImmaterialValueType is called when entering the immaterialValueType production.
	EnterImmaterialValueType(c *ImmaterialValueTypeContext)

	// EnterNullType is called when entering the nullType production.
	EnterNullType(c *NullTypeContext)

	// EnterEmptyType is called when entering the emptyType production.
	EnterEmptyType(c *EmptyTypeContext)

	// EnterGraphReferenceValueType is called when entering the graphReferenceValueType production.
	EnterGraphReferenceValueType(c *GraphReferenceValueTypeContext)

	// EnterClosedGraphReferenceValueType is called when entering the closedGraphReferenceValueType production.
	EnterClosedGraphReferenceValueType(c *ClosedGraphReferenceValueTypeContext)

	// EnterOpenGraphReferenceValueType is called when entering the openGraphReferenceValueType production.
	EnterOpenGraphReferenceValueType(c *OpenGraphReferenceValueTypeContext)

	// EnterBindingTableReferenceValueType is called when entering the bindingTableReferenceValueType production.
	EnterBindingTableReferenceValueType(c *BindingTableReferenceValueTypeContext)

	// EnterNodeReferenceValueType is called when entering the nodeReferenceValueType production.
	EnterNodeReferenceValueType(c *NodeReferenceValueTypeContext)

	// EnterClosedNodeReferenceValueType is called when entering the closedNodeReferenceValueType production.
	EnterClosedNodeReferenceValueType(c *ClosedNodeReferenceValueTypeContext)

	// EnterOpenNodeReferenceValueType is called when entering the openNodeReferenceValueType production.
	EnterOpenNodeReferenceValueType(c *OpenNodeReferenceValueTypeContext)

	// EnterEdgeReferenceValueType is called when entering the edgeReferenceValueType production.
	EnterEdgeReferenceValueType(c *EdgeReferenceValueTypeContext)

	// EnterClosedEdgeReferenceValueType is called when entering the closedEdgeReferenceValueType production.
	EnterClosedEdgeReferenceValueType(c *ClosedEdgeReferenceValueTypeContext)

	// EnterOpenEdgeReferenceValueType is called when entering the openEdgeReferenceValueType production.
	EnterOpenEdgeReferenceValueType(c *OpenEdgeReferenceValueTypeContext)

	// EnterPathValueType is called when entering the pathValueType production.
	EnterPathValueType(c *PathValueTypeContext)

	// EnterListValueTypeName is called when entering the listValueTypeName production.
	EnterListValueTypeName(c *ListValueTypeNameContext)

	// EnterListValueTypeNameSynonym is called when entering the listValueTypeNameSynonym production.
	EnterListValueTypeNameSynonym(c *ListValueTypeNameSynonymContext)

	// EnterRecordType is called when entering the recordType production.
	EnterRecordType(c *RecordTypeContext)

	// EnterFieldTypesSpecification is called when entering the fieldTypesSpecification production.
	EnterFieldTypesSpecification(c *FieldTypesSpecificationContext)

	// EnterFieldTypeList is called when entering the fieldTypeList production.
	EnterFieldTypeList(c *FieldTypeListContext)

	// EnterNotNull is called when entering the notNull production.
	EnterNotNull(c *NotNullContext)

	// EnterFieldType is called when entering the fieldType production.
	EnterFieldType(c *FieldTypeContext)

	// EnterSearchCondition is called when entering the searchCondition production.
	EnterSearchCondition(c *SearchConditionContext)

	// EnterPredicate is called when entering the predicate production.
	EnterPredicate(c *PredicateContext)

	// EnterCompOp is called when entering the compOp production.
	EnterCompOp(c *CompOpContext)

	// EnterExistsPredicate is called when entering the existsPredicate production.
	EnterExistsPredicate(c *ExistsPredicateContext)

	// EnterNullPredicate is called when entering the nullPredicate production.
	EnterNullPredicate(c *NullPredicateContext)

	// EnterNullPredicatePart2 is called when entering the nullPredicatePart2 production.
	EnterNullPredicatePart2(c *NullPredicatePart2Context)

	// EnterValueTypePredicate is called when entering the valueTypePredicate production.
	EnterValueTypePredicate(c *ValueTypePredicateContext)

	// EnterValueTypePredicatePart2 is called when entering the valueTypePredicatePart2 production.
	EnterValueTypePredicatePart2(c *ValueTypePredicatePart2Context)

	// EnterNormalizedPredicatePart2 is called when entering the normalizedPredicatePart2 production.
	EnterNormalizedPredicatePart2(c *NormalizedPredicatePart2Context)

	// EnterDirectedPredicate is called when entering the directedPredicate production.
	EnterDirectedPredicate(c *DirectedPredicateContext)

	// EnterDirectedPredicatePart2 is called when entering the directedPredicatePart2 production.
	EnterDirectedPredicatePart2(c *DirectedPredicatePart2Context)

	// EnterLabeledPredicate is called when entering the labeledPredicate production.
	EnterLabeledPredicate(c *LabeledPredicateContext)

	// EnterLabeledPredicatePart2 is called when entering the labeledPredicatePart2 production.
	EnterLabeledPredicatePart2(c *LabeledPredicatePart2Context)

	// EnterIsLabeledOrColon is called when entering the isLabeledOrColon production.
	EnterIsLabeledOrColon(c *IsLabeledOrColonContext)

	// EnterSourceDestinationPredicate is called when entering the sourceDestinationPredicate production.
	EnterSourceDestinationPredicate(c *SourceDestinationPredicateContext)

	// EnterNodeReference is called when entering the nodeReference production.
	EnterNodeReference(c *NodeReferenceContext)

	// EnterSourcePredicatePart2 is called when entering the sourcePredicatePart2 production.
	EnterSourcePredicatePart2(c *SourcePredicatePart2Context)

	// EnterDestinationPredicatePart2 is called when entering the destinationPredicatePart2 production.
	EnterDestinationPredicatePart2(c *DestinationPredicatePart2Context)

	// EnterEdgeReference is called when entering the edgeReference production.
	EnterEdgeReference(c *EdgeReferenceContext)

	// EnterAll_differentPredicate is called when entering the all_differentPredicate production.
	EnterAll_differentPredicate(c *All_differentPredicateContext)

	// EnterSamePredicate is called when entering the samePredicate production.
	EnterSamePredicate(c *SamePredicateContext)

	// EnterProperty_existsPredicate is called when entering the property_existsPredicate production.
	EnterProperty_existsPredicate(c *Property_existsPredicateContext)

	// EnterConjunctiveExprAlt is called when entering the conjunctiveExprAlt production.
	EnterConjunctiveExprAlt(c *ConjunctiveExprAltContext)

	// EnterPropertyGraphExprAlt is called when entering the propertyGraphExprAlt production.
	EnterPropertyGraphExprAlt(c *PropertyGraphExprAltContext)

	// EnterMultDivExprAlt is called when entering the multDivExprAlt production.
	EnterMultDivExprAlt(c *MultDivExprAltContext)

	// EnterBindingTableExprAlt is called when entering the bindingTableExprAlt production.
	EnterBindingTableExprAlt(c *BindingTableExprAltContext)

	// EnterSignedExprAlt is called when entering the signedExprAlt production.
	EnterSignedExprAlt(c *SignedExprAltContext)

	// EnterIsNotExprAlt is called when entering the isNotExprAlt production.
	EnterIsNotExprAlt(c *IsNotExprAltContext)

	// EnterNormalizedPredicateExprAlt is called when entering the normalizedPredicateExprAlt production.
	EnterNormalizedPredicateExprAlt(c *NormalizedPredicateExprAltContext)

	// EnterNotExprAlt is called when entering the notExprAlt production.
	EnterNotExprAlt(c *NotExprAltContext)

	// EnterValueFunctionExprAlt is called when entering the valueFunctionExprAlt production.
	EnterValueFunctionExprAlt(c *ValueFunctionExprAltContext)

	// EnterConcatenationExprAlt is called when entering the concatenationExprAlt production.
	EnterConcatenationExprAlt(c *ConcatenationExprAltContext)

	// EnterDisjunctiveExprAlt is called when entering the disjunctiveExprAlt production.
	EnterDisjunctiveExprAlt(c *DisjunctiveExprAltContext)

	// EnterComparisonExprAlt is called when entering the comparisonExprAlt production.
	EnterComparisonExprAlt(c *ComparisonExprAltContext)

	// EnterPrimaryExprAlt is called when entering the primaryExprAlt production.
	EnterPrimaryExprAlt(c *PrimaryExprAltContext)

	// EnterAddSubtractExprAlt is called when entering the addSubtractExprAlt production.
	EnterAddSubtractExprAlt(c *AddSubtractExprAltContext)

	// EnterPredicateExprAlt is called when entering the predicateExprAlt production.
	EnterPredicateExprAlt(c *PredicateExprAltContext)

	// EnterValueFunction is called when entering the valueFunction production.
	EnterValueFunction(c *ValueFunctionContext)

	// EnterBooleanValueExpression is called when entering the booleanValueExpression production.
	EnterBooleanValueExpression(c *BooleanValueExpressionContext)

	// EnterCharacterOrByteStringFunction is called when entering the characterOrByteStringFunction production.
	EnterCharacterOrByteStringFunction(c *CharacterOrByteStringFunctionContext)

	// EnterSubCharacterOrByteString is called when entering the subCharacterOrByteString production.
	EnterSubCharacterOrByteString(c *SubCharacterOrByteStringContext)

	// EnterTrimSingleCharacterOrByteString is called when entering the trimSingleCharacterOrByteString production.
	EnterTrimSingleCharacterOrByteString(c *TrimSingleCharacterOrByteStringContext)

	// EnterFoldCharacterString is called when entering the foldCharacterString production.
	EnterFoldCharacterString(c *FoldCharacterStringContext)

	// EnterTrimMultiCharacterCharacterString is called when entering the trimMultiCharacterCharacterString production.
	EnterTrimMultiCharacterCharacterString(c *TrimMultiCharacterCharacterStringContext)

	// EnterNormalizeCharacterString is called when entering the normalizeCharacterString production.
	EnterNormalizeCharacterString(c *NormalizeCharacterStringContext)

	// EnterNodeReferenceValueExpression is called when entering the nodeReferenceValueExpression production.
	EnterNodeReferenceValueExpression(c *NodeReferenceValueExpressionContext)

	// EnterEdgeReferenceValueExpression is called when entering the edgeReferenceValueExpression production.
	EnterEdgeReferenceValueExpression(c *EdgeReferenceValueExpressionContext)

	// EnterAggregatingValueExpression is called when entering the aggregatingValueExpression production.
	EnterAggregatingValueExpression(c *AggregatingValueExpressionContext)

	// EnterValueExpressionPrimary is called when entering the valueExpressionPrimary production.
	EnterValueExpressionPrimary(c *ValueExpressionPrimaryContext)

	// EnterParenthesizedValueExpression is called when entering the parenthesizedValueExpression production.
	EnterParenthesizedValueExpression(c *ParenthesizedValueExpressionContext)

	// EnterNonParenthesizedValueExpressionPrimary is called when entering the nonParenthesizedValueExpressionPrimary production.
	EnterNonParenthesizedValueExpressionPrimary(c *NonParenthesizedValueExpressionPrimaryContext)

	// EnterNonParenthesizedValueExpressionPrimarySpecialCase is called when entering the nonParenthesizedValueExpressionPrimarySpecialCase production.
	EnterNonParenthesizedValueExpressionPrimarySpecialCase(c *NonParenthesizedValueExpressionPrimarySpecialCaseContext)

	// EnterUnsignedValueSpecification is called when entering the unsignedValueSpecification production.
	EnterUnsignedValueSpecification(c *UnsignedValueSpecificationContext)

	// EnterNonNegativeIntegerSpecification is called when entering the nonNegativeIntegerSpecification production.
	EnterNonNegativeIntegerSpecification(c *NonNegativeIntegerSpecificationContext)

	// EnterGeneralValueSpecification is called when entering the generalValueSpecification production.
	EnterGeneralValueSpecification(c *GeneralValueSpecificationContext)

	// EnterDynamicParameterSpecification is called when entering the dynamicParameterSpecification production.
	EnterDynamicParameterSpecification(c *DynamicParameterSpecificationContext)

	// EnterLetValueExpression is called when entering the letValueExpression production.
	EnterLetValueExpression(c *LetValueExpressionContext)

	// EnterValueQueryExpression is called when entering the valueQueryExpression production.
	EnterValueQueryExpression(c *ValueQueryExpressionContext)

	// EnterCaseExpression is called when entering the caseExpression production.
	EnterCaseExpression(c *CaseExpressionContext)

	// EnterCaseAbbreviation is called when entering the caseAbbreviation production.
	EnterCaseAbbreviation(c *CaseAbbreviationContext)

	// EnterCaseSpecification is called when entering the caseSpecification production.
	EnterCaseSpecification(c *CaseSpecificationContext)

	// EnterSimpleCase is called when entering the simpleCase production.
	EnterSimpleCase(c *SimpleCaseContext)

	// EnterSearchedCase is called when entering the searchedCase production.
	EnterSearchedCase(c *SearchedCaseContext)

	// EnterSimpleWhenClause is called when entering the simpleWhenClause production.
	EnterSimpleWhenClause(c *SimpleWhenClauseContext)

	// EnterSearchedWhenClause is called when entering the searchedWhenClause production.
	EnterSearchedWhenClause(c *SearchedWhenClauseContext)

	// EnterElseClause is called when entering the elseClause production.
	EnterElseClause(c *ElseClauseContext)

	// EnterCaseOperand is called when entering the caseOperand production.
	EnterCaseOperand(c *CaseOperandContext)

	// EnterWhenOperandList is called when entering the whenOperandList production.
	EnterWhenOperandList(c *WhenOperandListContext)

	// EnterWhenOperand is called when entering the whenOperand production.
	EnterWhenOperand(c *WhenOperandContext)

	// EnterResult is called when entering the result production.
	EnterResult(c *ResultContext)

	// EnterResultExpression is called when entering the resultExpression production.
	EnterResultExpression(c *ResultExpressionContext)

	// EnterCastSpecification is called when entering the castSpecification production.
	EnterCastSpecification(c *CastSpecificationContext)

	// EnterCastOperand is called when entering the castOperand production.
	EnterCastOperand(c *CastOperandContext)

	// EnterCastTarget is called when entering the castTarget production.
	EnterCastTarget(c *CastTargetContext)

	// EnterAggregateFunction is called when entering the aggregateFunction production.
	EnterAggregateFunction(c *AggregateFunctionContext)

	// EnterGeneralSetFunction is called when entering the generalSetFunction production.
	EnterGeneralSetFunction(c *GeneralSetFunctionContext)

	// EnterBinarySetFunction is called when entering the binarySetFunction production.
	EnterBinarySetFunction(c *BinarySetFunctionContext)

	// EnterGeneralSetFunctionType is called when entering the generalSetFunctionType production.
	EnterGeneralSetFunctionType(c *GeneralSetFunctionTypeContext)

	// EnterSetQuantifier is called when entering the setQuantifier production.
	EnterSetQuantifier(c *SetQuantifierContext)

	// EnterBinarySetFunctionType is called when entering the binarySetFunctionType production.
	EnterBinarySetFunctionType(c *BinarySetFunctionTypeContext)

	// EnterDependentValueExpression is called when entering the dependentValueExpression production.
	EnterDependentValueExpression(c *DependentValueExpressionContext)

	// EnterIndependentValueExpression is called when entering the independentValueExpression production.
	EnterIndependentValueExpression(c *IndependentValueExpressionContext)

	// EnterElement_idFunction is called when entering the element_idFunction production.
	EnterElement_idFunction(c *Element_idFunctionContext)

	// EnterBindingVariableReference is called when entering the bindingVariableReference production.
	EnterBindingVariableReference(c *BindingVariableReferenceContext)

	// EnterPathValueExpression is called when entering the pathValueExpression production.
	EnterPathValueExpression(c *PathValueExpressionContext)

	// EnterPathValueConstructor is called when entering the pathValueConstructor production.
	EnterPathValueConstructor(c *PathValueConstructorContext)

	// EnterPathValueConstructorByEnumeration is called when entering the pathValueConstructorByEnumeration production.
	EnterPathValueConstructorByEnumeration(c *PathValueConstructorByEnumerationContext)

	// EnterPathElementList is called when entering the pathElementList production.
	EnterPathElementList(c *PathElementListContext)

	// EnterPathElementListStart is called when entering the pathElementListStart production.
	EnterPathElementListStart(c *PathElementListStartContext)

	// EnterPathElementListStep is called when entering the pathElementListStep production.
	EnterPathElementListStep(c *PathElementListStepContext)

	// EnterListValueExpression is called when entering the listValueExpression production.
	EnterListValueExpression(c *ListValueExpressionContext)

	// EnterListValueFunction is called when entering the listValueFunction production.
	EnterListValueFunction(c *ListValueFunctionContext)

	// EnterTrimListFunction is called when entering the trimListFunction production.
	EnterTrimListFunction(c *TrimListFunctionContext)

	// EnterElementsFunction is called when entering the elementsFunction production.
	EnterElementsFunction(c *ElementsFunctionContext)

	// EnterListValueConstructor is called when entering the listValueConstructor production.
	EnterListValueConstructor(c *ListValueConstructorContext)

	// EnterListValueConstructorByEnumeration is called when entering the listValueConstructorByEnumeration production.
	EnterListValueConstructorByEnumeration(c *ListValueConstructorByEnumerationContext)

	// EnterListElementList is called when entering the listElementList production.
	EnterListElementList(c *ListElementListContext)

	// EnterListElement is called when entering the listElement production.
	EnterListElement(c *ListElementContext)

	// EnterRecordConstructor is called when entering the recordConstructor production.
	EnterRecordConstructor(c *RecordConstructorContext)

	// EnterFieldsSpecification is called when entering the fieldsSpecification production.
	EnterFieldsSpecification(c *FieldsSpecificationContext)

	// EnterFieldList is called when entering the fieldList production.
	EnterFieldList(c *FieldListContext)

	// EnterField is called when entering the field production.
	EnterField(c *FieldContext)

	// EnterTruthValue is called when entering the truthValue production.
	EnterTruthValue(c *TruthValueContext)

	// EnterNumericValueExpression is called when entering the numericValueExpression production.
	EnterNumericValueExpression(c *NumericValueExpressionContext)

	// EnterNumericValueFunction is called when entering the numericValueFunction production.
	EnterNumericValueFunction(c *NumericValueFunctionContext)

	// EnterLengthExpression is called when entering the lengthExpression production.
	EnterLengthExpression(c *LengthExpressionContext)

	// EnterCardinalityExpression is called when entering the cardinalityExpression production.
	EnterCardinalityExpression(c *CardinalityExpressionContext)

	// EnterCardinalityExpressionArgument is called when entering the cardinalityExpressionArgument production.
	EnterCardinalityExpressionArgument(c *CardinalityExpressionArgumentContext)

	// EnterCharLengthExpression is called when entering the charLengthExpression production.
	EnterCharLengthExpression(c *CharLengthExpressionContext)

	// EnterByteLengthExpression is called when entering the byteLengthExpression production.
	EnterByteLengthExpression(c *ByteLengthExpressionContext)

	// EnterPathLengthExpression is called when entering the pathLengthExpression production.
	EnterPathLengthExpression(c *PathLengthExpressionContext)

	// EnterAbsoluteValueExpression is called when entering the absoluteValueExpression production.
	EnterAbsoluteValueExpression(c *AbsoluteValueExpressionContext)

	// EnterModulusExpression is called when entering the modulusExpression production.
	EnterModulusExpression(c *ModulusExpressionContext)

	// EnterNumericValueExpressionDividend is called when entering the numericValueExpressionDividend production.
	EnterNumericValueExpressionDividend(c *NumericValueExpressionDividendContext)

	// EnterNumericValueExpressionDivisor is called when entering the numericValueExpressionDivisor production.
	EnterNumericValueExpressionDivisor(c *NumericValueExpressionDivisorContext)

	// EnterTrigonometricFunction is called when entering the trigonometricFunction production.
	EnterTrigonometricFunction(c *TrigonometricFunctionContext)

	// EnterTrigonometricFunctionName is called when entering the trigonometricFunctionName production.
	EnterTrigonometricFunctionName(c *TrigonometricFunctionNameContext)

	// EnterGeneralLogarithmFunction is called when entering the generalLogarithmFunction production.
	EnterGeneralLogarithmFunction(c *GeneralLogarithmFunctionContext)

	// EnterGeneralLogarithmBase is called when entering the generalLogarithmBase production.
	EnterGeneralLogarithmBase(c *GeneralLogarithmBaseContext)

	// EnterGeneralLogarithmArgument is called when entering the generalLogarithmArgument production.
	EnterGeneralLogarithmArgument(c *GeneralLogarithmArgumentContext)

	// EnterCommonLogarithm is called when entering the commonLogarithm production.
	EnterCommonLogarithm(c *CommonLogarithmContext)

	// EnterNaturalLogarithm is called when entering the naturalLogarithm production.
	EnterNaturalLogarithm(c *NaturalLogarithmContext)

	// EnterExponentialFunction is called when entering the exponentialFunction production.
	EnterExponentialFunction(c *ExponentialFunctionContext)

	// EnterPowerFunction is called when entering the powerFunction production.
	EnterPowerFunction(c *PowerFunctionContext)

	// EnterNumericValueExpressionBase is called when entering the numericValueExpressionBase production.
	EnterNumericValueExpressionBase(c *NumericValueExpressionBaseContext)

	// EnterNumericValueExpressionExponent is called when entering the numericValueExpressionExponent production.
	EnterNumericValueExpressionExponent(c *NumericValueExpressionExponentContext)

	// EnterSquareRoot is called when entering the squareRoot production.
	EnterSquareRoot(c *SquareRootContext)

	// EnterFloorFunction is called when entering the floorFunction production.
	EnterFloorFunction(c *FloorFunctionContext)

	// EnterCeilingFunction is called when entering the ceilingFunction production.
	EnterCeilingFunction(c *CeilingFunctionContext)

	// EnterCharacterStringValueExpression is called when entering the characterStringValueExpression production.
	EnterCharacterStringValueExpression(c *CharacterStringValueExpressionContext)

	// EnterByteStringValueExpression is called when entering the byteStringValueExpression production.
	EnterByteStringValueExpression(c *ByteStringValueExpressionContext)

	// EnterTrimOperands is called when entering the trimOperands production.
	EnterTrimOperands(c *TrimOperandsContext)

	// EnterTrimCharacterOrByteStringSource is called when entering the trimCharacterOrByteStringSource production.
	EnterTrimCharacterOrByteStringSource(c *TrimCharacterOrByteStringSourceContext)

	// EnterTrimSpecification is called when entering the trimSpecification production.
	EnterTrimSpecification(c *TrimSpecificationContext)

	// EnterTrimCharacterOrByteString is called when entering the trimCharacterOrByteString production.
	EnterTrimCharacterOrByteString(c *TrimCharacterOrByteStringContext)

	// EnterNormalForm is called when entering the normalForm production.
	EnterNormalForm(c *NormalFormContext)

	// EnterStringLength is called when entering the stringLength production.
	EnterStringLength(c *StringLengthContext)

	// EnterDatetimeValueExpression is called when entering the datetimeValueExpression production.
	EnterDatetimeValueExpression(c *DatetimeValueExpressionContext)

	// EnterDatetimeValueFunction is called when entering the datetimeValueFunction production.
	EnterDatetimeValueFunction(c *DatetimeValueFunctionContext)

	// EnterDateFunction is called when entering the dateFunction production.
	EnterDateFunction(c *DateFunctionContext)

	// EnterTimeFunction is called when entering the timeFunction production.
	EnterTimeFunction(c *TimeFunctionContext)

	// EnterLocaltimeFunction is called when entering the localtimeFunction production.
	EnterLocaltimeFunction(c *LocaltimeFunctionContext)

	// EnterDatetimeFunction is called when entering the datetimeFunction production.
	EnterDatetimeFunction(c *DatetimeFunctionContext)

	// EnterLocaldatetimeFunction is called when entering the localdatetimeFunction production.
	EnterLocaldatetimeFunction(c *LocaldatetimeFunctionContext)

	// EnterDateFunctionParameters is called when entering the dateFunctionParameters production.
	EnterDateFunctionParameters(c *DateFunctionParametersContext)

	// EnterTimeFunctionParameters is called when entering the timeFunctionParameters production.
	EnterTimeFunctionParameters(c *TimeFunctionParametersContext)

	// EnterDatetimeFunctionParameters is called when entering the datetimeFunctionParameters production.
	EnterDatetimeFunctionParameters(c *DatetimeFunctionParametersContext)

	// EnterDurationValueExpression is called when entering the durationValueExpression production.
	EnterDurationValueExpression(c *DurationValueExpressionContext)

	// EnterDatetimeSubtraction is called when entering the datetimeSubtraction production.
	EnterDatetimeSubtraction(c *DatetimeSubtractionContext)

	// EnterDatetimeSubtractionParameters is called when entering the datetimeSubtractionParameters production.
	EnterDatetimeSubtractionParameters(c *DatetimeSubtractionParametersContext)

	// EnterDatetimeValueExpression1 is called when entering the datetimeValueExpression1 production.
	EnterDatetimeValueExpression1(c *DatetimeValueExpression1Context)

	// EnterDatetimeValueExpression2 is called when entering the datetimeValueExpression2 production.
	EnterDatetimeValueExpression2(c *DatetimeValueExpression2Context)

	// EnterDurationValueFunction is called when entering the durationValueFunction production.
	EnterDurationValueFunction(c *DurationValueFunctionContext)

	// EnterDurationFunction is called when entering the durationFunction production.
	EnterDurationFunction(c *DurationFunctionContext)

	// EnterDurationFunctionParameters is called when entering the durationFunctionParameters production.
	EnterDurationFunctionParameters(c *DurationFunctionParametersContext)

	// EnterObjectName is called when entering the objectName production.
	EnterObjectName(c *ObjectNameContext)

	// EnterObjectNameOrBindingVariable is called when entering the objectNameOrBindingVariable production.
	EnterObjectNameOrBindingVariable(c *ObjectNameOrBindingVariableContext)

	// EnterDirectoryName is called when entering the directoryName production.
	EnterDirectoryName(c *DirectoryNameContext)

	// EnterSchemaName is called when entering the schemaName production.
	EnterSchemaName(c *SchemaNameContext)

	// EnterGraphName is called when entering the graphName production.
	EnterGraphName(c *GraphNameContext)

	// EnterDelimitedGraphName is called when entering the delimitedGraphName production.
	EnterDelimitedGraphName(c *DelimitedGraphNameContext)

	// EnterGraphTypeName is called when entering the graphTypeName production.
	EnterGraphTypeName(c *GraphTypeNameContext)

	// EnterNodeTypeName is called when entering the nodeTypeName production.
	EnterNodeTypeName(c *NodeTypeNameContext)

	// EnterEdgeTypeName is called when entering the edgeTypeName production.
	EnterEdgeTypeName(c *EdgeTypeNameContext)

	// EnterBindingTableName is called when entering the bindingTableName production.
	EnterBindingTableName(c *BindingTableNameContext)

	// EnterDelimitedBindingTableName is called when entering the delimitedBindingTableName production.
	EnterDelimitedBindingTableName(c *DelimitedBindingTableNameContext)

	// EnterProcedureName is called when entering the procedureName production.
	EnterProcedureName(c *ProcedureNameContext)

	// EnterLabelName is called when entering the labelName production.
	EnterLabelName(c *LabelNameContext)

	// EnterPropertyName is called when entering the propertyName production.
	EnterPropertyName(c *PropertyNameContext)

	// EnterFieldName is called when entering the fieldName production.
	EnterFieldName(c *FieldNameContext)

	// EnterElementVariable is called when entering the elementVariable production.
	EnterElementVariable(c *ElementVariableContext)

	// EnterPathVariable is called when entering the pathVariable production.
	EnterPathVariable(c *PathVariableContext)

	// EnterSubpathVariable is called when entering the subpathVariable production.
	EnterSubpathVariable(c *SubpathVariableContext)

	// EnterBindingVariable is called when entering the bindingVariable production.
	EnterBindingVariable(c *BindingVariableContext)

	// EnterUnsignedLiteral is called when entering the unsignedLiteral production.
	EnterUnsignedLiteral(c *UnsignedLiteralContext)

	// EnterGeneralLiteral is called when entering the generalLiteral production.
	EnterGeneralLiteral(c *GeneralLiteralContext)

	// EnterTemporalLiteral is called when entering the temporalLiteral production.
	EnterTemporalLiteral(c *TemporalLiteralContext)

	// EnterDateLiteral is called when entering the dateLiteral production.
	EnterDateLiteral(c *DateLiteralContext)

	// EnterTimeLiteral is called when entering the timeLiteral production.
	EnterTimeLiteral(c *TimeLiteralContext)

	// EnterDatetimeLiteral is called when entering the datetimeLiteral production.
	EnterDatetimeLiteral(c *DatetimeLiteralContext)

	// EnterListLiteral is called when entering the listLiteral production.
	EnterListLiteral(c *ListLiteralContext)

	// EnterRecordLiteral is called when entering the recordLiteral production.
	EnterRecordLiteral(c *RecordLiteralContext)

	// EnterIdentifier is called when entering the identifier production.
	EnterIdentifier(c *IdentifierContext)

	// EnterRegularIdentifier is called when entering the regularIdentifier production.
	EnterRegularIdentifier(c *RegularIdentifierContext)

	// EnterTimeZoneString is called when entering the timeZoneString production.
	EnterTimeZoneString(c *TimeZoneStringContext)

	// EnterCharacterStringLiteral is called when entering the characterStringLiteral production.
	EnterCharacterStringLiteral(c *CharacterStringLiteralContext)

	// EnterUnsignedNumericLiteral is called when entering the unsignedNumericLiteral production.
	EnterUnsignedNumericLiteral(c *UnsignedNumericLiteralContext)

	// EnterExactNumericLiteral is called when entering the exactNumericLiteral production.
	EnterExactNumericLiteral(c *ExactNumericLiteralContext)

	// EnterApproximateNumericLiteral is called when entering the approximateNumericLiteral production.
	EnterApproximateNumericLiteral(c *ApproximateNumericLiteralContext)

	// EnterUnsignedInteger is called when entering the unsignedInteger production.
	EnterUnsignedInteger(c *UnsignedIntegerContext)

	// EnterUnsignedDecimalInteger is called when entering the unsignedDecimalInteger production.
	EnterUnsignedDecimalInteger(c *UnsignedDecimalIntegerContext)

	// EnterNullLiteral is called when entering the nullLiteral production.
	EnterNullLiteral(c *NullLiteralContext)

	// EnterDateString is called when entering the dateString production.
	EnterDateString(c *DateStringContext)

	// EnterTimeString is called when entering the timeString production.
	EnterTimeString(c *TimeStringContext)

	// EnterDatetimeString is called when entering the datetimeString production.
	EnterDatetimeString(c *DatetimeStringContext)

	// EnterDurationLiteral is called when entering the durationLiteral production.
	EnterDurationLiteral(c *DurationLiteralContext)

	// EnterDurationString is called when entering the durationString production.
	EnterDurationString(c *DurationStringContext)

	// EnterNodeSynonym is called when entering the nodeSynonym production.
	EnterNodeSynonym(c *NodeSynonymContext)

	// EnterEdgesSynonym is called when entering the edgesSynonym production.
	EnterEdgesSynonym(c *EdgesSynonymContext)

	// EnterEdgeSynonym is called when entering the edgeSynonym production.
	EnterEdgeSynonym(c *EdgeSynonymContext)

	// EnterNonReservedWords is called when entering the nonReservedWords production.
	EnterNonReservedWords(c *NonReservedWordsContext)

	// ExitGqlProgram is called when exiting the gqlProgram production.
	ExitGqlProgram(c *GqlProgramContext)

	// ExitProgramActivity is called when exiting the programActivity production.
	ExitProgramActivity(c *ProgramActivityContext)

	// ExitSessionActivity is called when exiting the sessionActivity production.
	ExitSessionActivity(c *SessionActivityContext)

	// ExitTransactionActivity is called when exiting the transactionActivity production.
	ExitTransactionActivity(c *TransactionActivityContext)

	// ExitEndTransactionCommand is called when exiting the endTransactionCommand production.
	ExitEndTransactionCommand(c *EndTransactionCommandContext)

	// ExitSessionSetCommand is called when exiting the sessionSetCommand production.
	ExitSessionSetCommand(c *SessionSetCommandContext)

	// ExitSessionSetSchemaClause is called when exiting the sessionSetSchemaClause production.
	ExitSessionSetSchemaClause(c *SessionSetSchemaClauseContext)

	// ExitSessionSetGraphClause is called when exiting the sessionSetGraphClause production.
	ExitSessionSetGraphClause(c *SessionSetGraphClauseContext)

	// ExitSessionSetTimeZoneClause is called when exiting the sessionSetTimeZoneClause production.
	ExitSessionSetTimeZoneClause(c *SessionSetTimeZoneClauseContext)

	// ExitSetTimeZoneValue is called when exiting the setTimeZoneValue production.
	ExitSetTimeZoneValue(c *SetTimeZoneValueContext)

	// ExitSessionSetParameterClause is called when exiting the sessionSetParameterClause production.
	ExitSessionSetParameterClause(c *SessionSetParameterClauseContext)

	// ExitSessionSetGraphParameterClause is called when exiting the sessionSetGraphParameterClause production.
	ExitSessionSetGraphParameterClause(c *SessionSetGraphParameterClauseContext)

	// ExitSessionSetBindingTableParameterClause is called when exiting the sessionSetBindingTableParameterClause production.
	ExitSessionSetBindingTableParameterClause(c *SessionSetBindingTableParameterClauseContext)

	// ExitSessionSetValueParameterClause is called when exiting the sessionSetValueParameterClause production.
	ExitSessionSetValueParameterClause(c *SessionSetValueParameterClauseContext)

	// ExitSessionSetParameterName is called when exiting the sessionSetParameterName production.
	ExitSessionSetParameterName(c *SessionSetParameterNameContext)

	// ExitSessionResetCommand is called when exiting the sessionResetCommand production.
	ExitSessionResetCommand(c *SessionResetCommandContext)

	// ExitSessionResetArguments is called when exiting the sessionResetArguments production.
	ExitSessionResetArguments(c *SessionResetArgumentsContext)

	// ExitSessionCloseCommand is called when exiting the sessionCloseCommand production.
	ExitSessionCloseCommand(c *SessionCloseCommandContext)

	// ExitSessionParameterSpecification is called when exiting the sessionParameterSpecification production.
	ExitSessionParameterSpecification(c *SessionParameterSpecificationContext)

	// ExitStartTransactionCommand is called when exiting the startTransactionCommand production.
	ExitStartTransactionCommand(c *StartTransactionCommandContext)

	// ExitTransactionCharacteristics is called when exiting the transactionCharacteristics production.
	ExitTransactionCharacteristics(c *TransactionCharacteristicsContext)

	// ExitTransactionMode is called when exiting the transactionMode production.
	ExitTransactionMode(c *TransactionModeContext)

	// ExitTransactionAccessMode is called when exiting the transactionAccessMode production.
	ExitTransactionAccessMode(c *TransactionAccessModeContext)

	// ExitRollbackCommand is called when exiting the rollbackCommand production.
	ExitRollbackCommand(c *RollbackCommandContext)

	// ExitCommitCommand is called when exiting the commitCommand production.
	ExitCommitCommand(c *CommitCommandContext)

	// ExitNestedProcedureSpecification is called when exiting the nestedProcedureSpecification production.
	ExitNestedProcedureSpecification(c *NestedProcedureSpecificationContext)

	// ExitProcedureSpecification is called when exiting the procedureSpecification production.
	ExitProcedureSpecification(c *ProcedureSpecificationContext)

	// ExitNestedDataModifyingProcedureSpecification is called when exiting the nestedDataModifyingProcedureSpecification production.
	ExitNestedDataModifyingProcedureSpecification(c *NestedDataModifyingProcedureSpecificationContext)

	// ExitNestedQuerySpecification is called when exiting the nestedQuerySpecification production.
	ExitNestedQuerySpecification(c *NestedQuerySpecificationContext)

	// ExitProcedureBody is called when exiting the procedureBody production.
	ExitProcedureBody(c *ProcedureBodyContext)

	// ExitBindingVariableDefinitionBlock is called when exiting the bindingVariableDefinitionBlock production.
	ExitBindingVariableDefinitionBlock(c *BindingVariableDefinitionBlockContext)

	// ExitBindingVariableDefinition is called when exiting the bindingVariableDefinition production.
	ExitBindingVariableDefinition(c *BindingVariableDefinitionContext)

	// ExitStatementBlock is called when exiting the statementBlock production.
	ExitStatementBlock(c *StatementBlockContext)

	// ExitStatement is called when exiting the statement production.
	ExitStatement(c *StatementContext)

	// ExitNextStatement is called when exiting the nextStatement production.
	ExitNextStatement(c *NextStatementContext)

	// ExitGraphVariableDefinition is called when exiting the graphVariableDefinition production.
	ExitGraphVariableDefinition(c *GraphVariableDefinitionContext)

	// ExitOptTypedGraphInitializer is called when exiting the optTypedGraphInitializer production.
	ExitOptTypedGraphInitializer(c *OptTypedGraphInitializerContext)

	// ExitGraphInitializer is called when exiting the graphInitializer production.
	ExitGraphInitializer(c *GraphInitializerContext)

	// ExitBindingTableVariableDefinition is called when exiting the bindingTableVariableDefinition production.
	ExitBindingTableVariableDefinition(c *BindingTableVariableDefinitionContext)

	// ExitOptTypedBindingTableInitializer is called when exiting the optTypedBindingTableInitializer production.
	ExitOptTypedBindingTableInitializer(c *OptTypedBindingTableInitializerContext)

	// ExitBindingTableInitializer is called when exiting the bindingTableInitializer production.
	ExitBindingTableInitializer(c *BindingTableInitializerContext)

	// ExitValueVariableDefinition is called when exiting the valueVariableDefinition production.
	ExitValueVariableDefinition(c *ValueVariableDefinitionContext)

	// ExitOptTypedValueInitializer is called when exiting the optTypedValueInitializer production.
	ExitOptTypedValueInitializer(c *OptTypedValueInitializerContext)

	// ExitValueInitializer is called when exiting the valueInitializer production.
	ExitValueInitializer(c *ValueInitializerContext)

	// ExitGraphExpression is called when exiting the graphExpression production.
	ExitGraphExpression(c *GraphExpressionContext)

	// ExitCurrentGraph is called when exiting the currentGraph production.
	ExitCurrentGraph(c *CurrentGraphContext)

	// ExitBindingTableExpression is called when exiting the bindingTableExpression production.
	ExitBindingTableExpression(c *BindingTableExpressionContext)

	// ExitNestedBindingTableQuerySpecification is called when exiting the nestedBindingTableQuerySpecification production.
	ExitNestedBindingTableQuerySpecification(c *NestedBindingTableQuerySpecificationContext)

	// ExitObjectExpressionPrimary is called when exiting the objectExpressionPrimary production.
	ExitObjectExpressionPrimary(c *ObjectExpressionPrimaryContext)

	// ExitLinearCatalogModifyingStatement is called when exiting the linearCatalogModifyingStatement production.
	ExitLinearCatalogModifyingStatement(c *LinearCatalogModifyingStatementContext)

	// ExitSimpleCatalogModifyingStatement is called when exiting the simpleCatalogModifyingStatement production.
	ExitSimpleCatalogModifyingStatement(c *SimpleCatalogModifyingStatementContext)

	// ExitPrimitiveCatalogModifyingStatement is called when exiting the primitiveCatalogModifyingStatement production.
	ExitPrimitiveCatalogModifyingStatement(c *PrimitiveCatalogModifyingStatementContext)

	// ExitCreateSchemaStatement is called when exiting the createSchemaStatement production.
	ExitCreateSchemaStatement(c *CreateSchemaStatementContext)

	// ExitDropSchemaStatement is called when exiting the dropSchemaStatement production.
	ExitDropSchemaStatement(c *DropSchemaStatementContext)

	// ExitCreateGraphStatement is called when exiting the createGraphStatement production.
	ExitCreateGraphStatement(c *CreateGraphStatementContext)

	// ExitOpenGraphType is called when exiting the openGraphType production.
	ExitOpenGraphType(c *OpenGraphTypeContext)

	// ExitOfGraphType is called when exiting the ofGraphType production.
	ExitOfGraphType(c *OfGraphTypeContext)

	// ExitGraphTypeLikeGraph is called when exiting the graphTypeLikeGraph production.
	ExitGraphTypeLikeGraph(c *GraphTypeLikeGraphContext)

	// ExitGraphSource is called when exiting the graphSource production.
	ExitGraphSource(c *GraphSourceContext)

	// ExitDropGraphStatement is called when exiting the dropGraphStatement production.
	ExitDropGraphStatement(c *DropGraphStatementContext)

	// ExitCreateGraphTypeStatement is called when exiting the createGraphTypeStatement production.
	ExitCreateGraphTypeStatement(c *CreateGraphTypeStatementContext)

	// ExitGraphTypeSource is called when exiting the graphTypeSource production.
	ExitGraphTypeSource(c *GraphTypeSourceContext)

	// ExitCopyOfGraphType is called when exiting the copyOfGraphType production.
	ExitCopyOfGraphType(c *CopyOfGraphTypeContext)

	// ExitDropGraphTypeStatement is called when exiting the dropGraphTypeStatement production.
	ExitDropGraphTypeStatement(c *DropGraphTypeStatementContext)

	// ExitCallCatalogModifyingProcedureStatement is called when exiting the callCatalogModifyingProcedureStatement production.
	ExitCallCatalogModifyingProcedureStatement(c *CallCatalogModifyingProcedureStatementContext)

	// ExitLinearDataModifyingStatement is called when exiting the linearDataModifyingStatement production.
	ExitLinearDataModifyingStatement(c *LinearDataModifyingStatementContext)

	// ExitFocusedLinearDataModifyingStatement is called when exiting the focusedLinearDataModifyingStatement production.
	ExitFocusedLinearDataModifyingStatement(c *FocusedLinearDataModifyingStatementContext)

	// ExitFocusedLinearDataModifyingStatementBody is called when exiting the focusedLinearDataModifyingStatementBody production.
	ExitFocusedLinearDataModifyingStatementBody(c *FocusedLinearDataModifyingStatementBodyContext)

	// ExitFocusedNestedDataModifyingProcedureSpecification is called when exiting the focusedNestedDataModifyingProcedureSpecification production.
	ExitFocusedNestedDataModifyingProcedureSpecification(c *FocusedNestedDataModifyingProcedureSpecificationContext)

	// ExitAmbientLinearDataModifyingStatement is called when exiting the ambientLinearDataModifyingStatement production.
	ExitAmbientLinearDataModifyingStatement(c *AmbientLinearDataModifyingStatementContext)

	// ExitAmbientLinearDataModifyingStatementBody is called when exiting the ambientLinearDataModifyingStatementBody production.
	ExitAmbientLinearDataModifyingStatementBody(c *AmbientLinearDataModifyingStatementBodyContext)

	// ExitSimpleLinearDataAccessingStatement is called when exiting the simpleLinearDataAccessingStatement production.
	ExitSimpleLinearDataAccessingStatement(c *SimpleLinearDataAccessingStatementContext)

	// ExitSimpleDataAccessingStatement is called when exiting the simpleDataAccessingStatement production.
	ExitSimpleDataAccessingStatement(c *SimpleDataAccessingStatementContext)

	// ExitSimpleDataModifyingStatement is called when exiting the simpleDataModifyingStatement production.
	ExitSimpleDataModifyingStatement(c *SimpleDataModifyingStatementContext)

	// ExitPrimitiveDataModifyingStatement is called when exiting the primitiveDataModifyingStatement production.
	ExitPrimitiveDataModifyingStatement(c *PrimitiveDataModifyingStatementContext)

	// ExitInsertStatement is called when exiting the insertStatement production.
	ExitInsertStatement(c *InsertStatementContext)

	// ExitSetStatement is called when exiting the setStatement production.
	ExitSetStatement(c *SetStatementContext)

	// ExitSetItemList is called when exiting the setItemList production.
	ExitSetItemList(c *SetItemListContext)

	// ExitSetItem is called when exiting the setItem production.
	ExitSetItem(c *SetItemContext)

	// ExitSetPropertyItem is called when exiting the setPropertyItem production.
	ExitSetPropertyItem(c *SetPropertyItemContext)

	// ExitSetAllPropertiesItem is called when exiting the setAllPropertiesItem production.
	ExitSetAllPropertiesItem(c *SetAllPropertiesItemContext)

	// ExitSetLabelItem is called when exiting the setLabelItem production.
	ExitSetLabelItem(c *SetLabelItemContext)

	// ExitRemoveStatement is called when exiting the removeStatement production.
	ExitRemoveStatement(c *RemoveStatementContext)

	// ExitRemoveItemList is called when exiting the removeItemList production.
	ExitRemoveItemList(c *RemoveItemListContext)

	// ExitRemoveItem is called when exiting the removeItem production.
	ExitRemoveItem(c *RemoveItemContext)

	// ExitRemovePropertyItem is called when exiting the removePropertyItem production.
	ExitRemovePropertyItem(c *RemovePropertyItemContext)

	// ExitRemoveLabelItem is called when exiting the removeLabelItem production.
	ExitRemoveLabelItem(c *RemoveLabelItemContext)

	// ExitDeleteStatement is called when exiting the deleteStatement production.
	ExitDeleteStatement(c *DeleteStatementContext)

	// ExitDeleteItemList is called when exiting the deleteItemList production.
	ExitDeleteItemList(c *DeleteItemListContext)

	// ExitDeleteItem is called when exiting the deleteItem production.
	ExitDeleteItem(c *DeleteItemContext)

	// ExitCallDataModifyingProcedureStatement is called when exiting the callDataModifyingProcedureStatement production.
	ExitCallDataModifyingProcedureStatement(c *CallDataModifyingProcedureStatementContext)

	// ExitCompositeQueryStatement is called when exiting the compositeQueryStatement production.
	ExitCompositeQueryStatement(c *CompositeQueryStatementContext)

	// ExitCompositeQueryExpression is called when exiting the compositeQueryExpression production.
	ExitCompositeQueryExpression(c *CompositeQueryExpressionContext)

	// ExitQueryConjunction is called when exiting the queryConjunction production.
	ExitQueryConjunction(c *QueryConjunctionContext)

	// ExitSetOperator is called when exiting the setOperator production.
	ExitSetOperator(c *SetOperatorContext)

	// ExitCompositeQueryPrimary is called when exiting the compositeQueryPrimary production.
	ExitCompositeQueryPrimary(c *CompositeQueryPrimaryContext)

	// ExitLinearQueryStatement is called when exiting the linearQueryStatement production.
	ExitLinearQueryStatement(c *LinearQueryStatementContext)

	// ExitFocusedLinearQueryStatement is called when exiting the focusedLinearQueryStatement production.
	ExitFocusedLinearQueryStatement(c *FocusedLinearQueryStatementContext)

	// ExitFocusedLinearQueryStatementPart is called when exiting the focusedLinearQueryStatementPart production.
	ExitFocusedLinearQueryStatementPart(c *FocusedLinearQueryStatementPartContext)

	// ExitFocusedLinearQueryAndPrimitiveResultStatementPart is called when exiting the focusedLinearQueryAndPrimitiveResultStatementPart production.
	ExitFocusedLinearQueryAndPrimitiveResultStatementPart(c *FocusedLinearQueryAndPrimitiveResultStatementPartContext)

	// ExitFocusedPrimitiveResultStatement is called when exiting the focusedPrimitiveResultStatement production.
	ExitFocusedPrimitiveResultStatement(c *FocusedPrimitiveResultStatementContext)

	// ExitFocusedNestedQuerySpecification is called when exiting the focusedNestedQuerySpecification production.
	ExitFocusedNestedQuerySpecification(c *FocusedNestedQuerySpecificationContext)

	// ExitAmbientLinearQueryStatement is called when exiting the ambientLinearQueryStatement production.
	ExitAmbientLinearQueryStatement(c *AmbientLinearQueryStatementContext)

	// ExitSimpleLinearQueryStatement is called when exiting the simpleLinearQueryStatement production.
	ExitSimpleLinearQueryStatement(c *SimpleLinearQueryStatementContext)

	// ExitSimpleQueryStatement is called when exiting the simpleQueryStatement production.
	ExitSimpleQueryStatement(c *SimpleQueryStatementContext)

	// ExitPrimitiveQueryStatement is called when exiting the primitiveQueryStatement production.
	ExitPrimitiveQueryStatement(c *PrimitiveQueryStatementContext)

	// ExitMatchStatement is called when exiting the matchStatement production.
	ExitMatchStatement(c *MatchStatementContext)

	// ExitSimpleMatchStatement is called when exiting the simpleMatchStatement production.
	ExitSimpleMatchStatement(c *SimpleMatchStatementContext)

	// ExitOptionalMatchStatement is called when exiting the optionalMatchStatement production.
	ExitOptionalMatchStatement(c *OptionalMatchStatementContext)

	// ExitOptionalOperand is called when exiting the optionalOperand production.
	ExitOptionalOperand(c *OptionalOperandContext)

	// ExitMatchStatementBlock is called when exiting the matchStatementBlock production.
	ExitMatchStatementBlock(c *MatchStatementBlockContext)

	// ExitCallQueryStatement is called when exiting the callQueryStatement production.
	ExitCallQueryStatement(c *CallQueryStatementContext)

	// ExitFilterStatement is called when exiting the filterStatement production.
	ExitFilterStatement(c *FilterStatementContext)

	// ExitLetStatement is called when exiting the letStatement production.
	ExitLetStatement(c *LetStatementContext)

	// ExitLetVariableDefinitionList is called when exiting the letVariableDefinitionList production.
	ExitLetVariableDefinitionList(c *LetVariableDefinitionListContext)

	// ExitLetVariableDefinition is called when exiting the letVariableDefinition production.
	ExitLetVariableDefinition(c *LetVariableDefinitionContext)

	// ExitForStatement is called when exiting the forStatement production.
	ExitForStatement(c *ForStatementContext)

	// ExitForItem is called when exiting the forItem production.
	ExitForItem(c *ForItemContext)

	// ExitForItemAlias is called when exiting the forItemAlias production.
	ExitForItemAlias(c *ForItemAliasContext)

	// ExitForItemSource is called when exiting the forItemSource production.
	ExitForItemSource(c *ForItemSourceContext)

	// ExitForOrdinalityOrOffset is called when exiting the forOrdinalityOrOffset production.
	ExitForOrdinalityOrOffset(c *ForOrdinalityOrOffsetContext)

	// ExitOrderByAndPageStatement is called when exiting the orderByAndPageStatement production.
	ExitOrderByAndPageStatement(c *OrderByAndPageStatementContext)

	// ExitPrimitiveResultStatement is called when exiting the primitiveResultStatement production.
	ExitPrimitiveResultStatement(c *PrimitiveResultStatementContext)

	// ExitReturnStatement is called when exiting the returnStatement production.
	ExitReturnStatement(c *ReturnStatementContext)

	// ExitReturnStatementBody is called when exiting the returnStatementBody production.
	ExitReturnStatementBody(c *ReturnStatementBodyContext)

	// ExitReturnItemList is called when exiting the returnItemList production.
	ExitReturnItemList(c *ReturnItemListContext)

	// ExitReturnItem is called when exiting the returnItem production.
	ExitReturnItem(c *ReturnItemContext)

	// ExitReturnItemAlias is called when exiting the returnItemAlias production.
	ExitReturnItemAlias(c *ReturnItemAliasContext)

	// ExitSelectStatement is called when exiting the selectStatement production.
	ExitSelectStatement(c *SelectStatementContext)

	// ExitSelectItemList is called when exiting the selectItemList production.
	ExitSelectItemList(c *SelectItemListContext)

	// ExitSelectItem is called when exiting the selectItem production.
	ExitSelectItem(c *SelectItemContext)

	// ExitSelectItemAlias is called when exiting the selectItemAlias production.
	ExitSelectItemAlias(c *SelectItemAliasContext)

	// ExitHavingClause is called when exiting the havingClause production.
	ExitHavingClause(c *HavingClauseContext)

	// ExitSelectStatementBody is called when exiting the selectStatementBody production.
	ExitSelectStatementBody(c *SelectStatementBodyContext)

	// ExitSelectGraphMatchList is called when exiting the selectGraphMatchList production.
	ExitSelectGraphMatchList(c *SelectGraphMatchListContext)

	// ExitSelectGraphMatch is called when exiting the selectGraphMatch production.
	ExitSelectGraphMatch(c *SelectGraphMatchContext)

	// ExitSelectQuerySpecification is called when exiting the selectQuerySpecification production.
	ExitSelectQuerySpecification(c *SelectQuerySpecificationContext)

	// ExitCallProcedureStatement is called when exiting the callProcedureStatement production.
	ExitCallProcedureStatement(c *CallProcedureStatementContext)

	// ExitProcedureCall is called when exiting the procedureCall production.
	ExitProcedureCall(c *ProcedureCallContext)

	// ExitInlineProcedureCall is called when exiting the inlineProcedureCall production.
	ExitInlineProcedureCall(c *InlineProcedureCallContext)

	// ExitVariableScopeClause is called when exiting the variableScopeClause production.
	ExitVariableScopeClause(c *VariableScopeClauseContext)

	// ExitBindingVariableReferenceList is called when exiting the bindingVariableReferenceList production.
	ExitBindingVariableReferenceList(c *BindingVariableReferenceListContext)

	// ExitNamedProcedureCall is called when exiting the namedProcedureCall production.
	ExitNamedProcedureCall(c *NamedProcedureCallContext)

	// ExitProcedureArgumentList is called when exiting the procedureArgumentList production.
	ExitProcedureArgumentList(c *ProcedureArgumentListContext)

	// ExitProcedureArgument is called when exiting the procedureArgument production.
	ExitProcedureArgument(c *ProcedureArgumentContext)

	// ExitAtSchemaClause is called when exiting the atSchemaClause production.
	ExitAtSchemaClause(c *AtSchemaClauseContext)

	// ExitUseGraphClause is called when exiting the useGraphClause production.
	ExitUseGraphClause(c *UseGraphClauseContext)

	// ExitGraphPatternBindingTable is called when exiting the graphPatternBindingTable production.
	ExitGraphPatternBindingTable(c *GraphPatternBindingTableContext)

	// ExitGraphPatternYieldClause is called when exiting the graphPatternYieldClause production.
	ExitGraphPatternYieldClause(c *GraphPatternYieldClauseContext)

	// ExitGraphPatternYieldItemList is called when exiting the graphPatternYieldItemList production.
	ExitGraphPatternYieldItemList(c *GraphPatternYieldItemListContext)

	// ExitGraphPatternYieldItem is called when exiting the graphPatternYieldItem production.
	ExitGraphPatternYieldItem(c *GraphPatternYieldItemContext)

	// ExitGraphPattern is called when exiting the graphPattern production.
	ExitGraphPattern(c *GraphPatternContext)

	// ExitMatchMode is called when exiting the matchMode production.
	ExitMatchMode(c *MatchModeContext)

	// ExitRepeatableElementsMatchMode is called when exiting the repeatableElementsMatchMode production.
	ExitRepeatableElementsMatchMode(c *RepeatableElementsMatchModeContext)

	// ExitDifferentEdgesMatchMode is called when exiting the differentEdgesMatchMode production.
	ExitDifferentEdgesMatchMode(c *DifferentEdgesMatchModeContext)

	// ExitElementBindingsOrElements is called when exiting the elementBindingsOrElements production.
	ExitElementBindingsOrElements(c *ElementBindingsOrElementsContext)

	// ExitEdgeBindingsOrEdges is called when exiting the edgeBindingsOrEdges production.
	ExitEdgeBindingsOrEdges(c *EdgeBindingsOrEdgesContext)

	// ExitPathPatternList is called when exiting the pathPatternList production.
	ExitPathPatternList(c *PathPatternListContext)

	// ExitPathPattern is called when exiting the pathPattern production.
	ExitPathPattern(c *PathPatternContext)

	// ExitPathVariableDeclaration is called when exiting the pathVariableDeclaration production.
	ExitPathVariableDeclaration(c *PathVariableDeclarationContext)

	// ExitKeepClause is called when exiting the keepClause production.
	ExitKeepClause(c *KeepClauseContext)

	// ExitGraphPatternWhereClause is called when exiting the graphPatternWhereClause production.
	ExitGraphPatternWhereClause(c *GraphPatternWhereClauseContext)

	// ExitInsertGraphPattern is called when exiting the insertGraphPattern production.
	ExitInsertGraphPattern(c *InsertGraphPatternContext)

	// ExitInsertPathPatternList is called when exiting the insertPathPatternList production.
	ExitInsertPathPatternList(c *InsertPathPatternListContext)

	// ExitInsertPathPattern is called when exiting the insertPathPattern production.
	ExitInsertPathPattern(c *InsertPathPatternContext)

	// ExitInsertNodePattern is called when exiting the insertNodePattern production.
	ExitInsertNodePattern(c *InsertNodePatternContext)

	// ExitInsertEdgePattern is called when exiting the insertEdgePattern production.
	ExitInsertEdgePattern(c *InsertEdgePatternContext)

	// ExitInsertEdgePointingLeft is called when exiting the insertEdgePointingLeft production.
	ExitInsertEdgePointingLeft(c *InsertEdgePointingLeftContext)

	// ExitInsertEdgePointingRight is called when exiting the insertEdgePointingRight production.
	ExitInsertEdgePointingRight(c *InsertEdgePointingRightContext)

	// ExitInsertEdgeUndirected is called when exiting the insertEdgeUndirected production.
	ExitInsertEdgeUndirected(c *InsertEdgeUndirectedContext)

	// ExitInsertElementPatternFiller is called when exiting the insertElementPatternFiller production.
	ExitInsertElementPatternFiller(c *InsertElementPatternFillerContext)

	// ExitLabelAndPropertySetSpecification is called when exiting the labelAndPropertySetSpecification production.
	ExitLabelAndPropertySetSpecification(c *LabelAndPropertySetSpecificationContext)

	// ExitPathPatternPrefix is called when exiting the pathPatternPrefix production.
	ExitPathPatternPrefix(c *PathPatternPrefixContext)

	// ExitPathModePrefix is called when exiting the pathModePrefix production.
	ExitPathModePrefix(c *PathModePrefixContext)

	// ExitPathMode is called when exiting the pathMode production.
	ExitPathMode(c *PathModeContext)

	// ExitPathSearchPrefix is called when exiting the pathSearchPrefix production.
	ExitPathSearchPrefix(c *PathSearchPrefixContext)

	// ExitAllPathSearch is called when exiting the allPathSearch production.
	ExitAllPathSearch(c *AllPathSearchContext)

	// ExitPathOrPaths is called when exiting the pathOrPaths production.
	ExitPathOrPaths(c *PathOrPathsContext)

	// ExitAnyPathSearch is called when exiting the anyPathSearch production.
	ExitAnyPathSearch(c *AnyPathSearchContext)

	// ExitNumberOfPaths is called when exiting the numberOfPaths production.
	ExitNumberOfPaths(c *NumberOfPathsContext)

	// ExitShortestPathSearch is called when exiting the shortestPathSearch production.
	ExitShortestPathSearch(c *ShortestPathSearchContext)

	// ExitAllShortestPathSearch is called when exiting the allShortestPathSearch production.
	ExitAllShortestPathSearch(c *AllShortestPathSearchContext)

	// ExitAnyShortestPathSearch is called when exiting the anyShortestPathSearch production.
	ExitAnyShortestPathSearch(c *AnyShortestPathSearchContext)

	// ExitCountedShortestPathSearch is called when exiting the countedShortestPathSearch production.
	ExitCountedShortestPathSearch(c *CountedShortestPathSearchContext)

	// ExitCountedShortestGroupSearch is called when exiting the countedShortestGroupSearch production.
	ExitCountedShortestGroupSearch(c *CountedShortestGroupSearchContext)

	// ExitNumberOfGroups is called when exiting the numberOfGroups production.
	ExitNumberOfGroups(c *NumberOfGroupsContext)

	// ExitPpePathTerm is called when exiting the ppePathTerm production.
	ExitPpePathTerm(c *PpePathTermContext)

	// ExitPpeMultisetAlternation is called when exiting the ppeMultisetAlternation production.
	ExitPpeMultisetAlternation(c *PpeMultisetAlternationContext)

	// ExitPpePatternUnion is called when exiting the ppePatternUnion production.
	ExitPpePatternUnion(c *PpePatternUnionContext)

	// ExitPathTerm is called when exiting the pathTerm production.
	ExitPathTerm(c *PathTermContext)

	// ExitPfPathPrimary is called when exiting the pfPathPrimary production.
	ExitPfPathPrimary(c *PfPathPrimaryContext)

	// ExitPfQuantifiedPathPrimary is called when exiting the pfQuantifiedPathPrimary production.
	ExitPfQuantifiedPathPrimary(c *PfQuantifiedPathPrimaryContext)

	// ExitPfQuestionedPathPrimary is called when exiting the pfQuestionedPathPrimary production.
	ExitPfQuestionedPathPrimary(c *PfQuestionedPathPrimaryContext)

	// ExitPpElementPattern is called when exiting the ppElementPattern production.
	ExitPpElementPattern(c *PpElementPatternContext)

	// ExitPpParenthesizedPathPatternExpression is called when exiting the ppParenthesizedPathPatternExpression production.
	ExitPpParenthesizedPathPatternExpression(c *PpParenthesizedPathPatternExpressionContext)

	// ExitPpSimplifiedPathPatternExpression is called when exiting the ppSimplifiedPathPatternExpression production.
	ExitPpSimplifiedPathPatternExpression(c *PpSimplifiedPathPatternExpressionContext)

	// ExitElementPattern is called when exiting the elementPattern production.
	ExitElementPattern(c *ElementPatternContext)

	// ExitNodePattern is called when exiting the nodePattern production.
	ExitNodePattern(c *NodePatternContext)

	// ExitElementPatternFiller is called when exiting the elementPatternFiller production.
	ExitElementPatternFiller(c *ElementPatternFillerContext)

	// ExitElementVariableDeclaration is called when exiting the elementVariableDeclaration production.
	ExitElementVariableDeclaration(c *ElementVariableDeclarationContext)

	// ExitIsLabelExpression is called when exiting the isLabelExpression production.
	ExitIsLabelExpression(c *IsLabelExpressionContext)

	// ExitIsOrColon is called when exiting the isOrColon production.
	ExitIsOrColon(c *IsOrColonContext)

	// ExitElementPatternPredicate is called when exiting the elementPatternPredicate production.
	ExitElementPatternPredicate(c *ElementPatternPredicateContext)

	// ExitElementPatternWhereClause is called when exiting the elementPatternWhereClause production.
	ExitElementPatternWhereClause(c *ElementPatternWhereClauseContext)

	// ExitElementPropertySpecification is called when exiting the elementPropertySpecification production.
	ExitElementPropertySpecification(c *ElementPropertySpecificationContext)

	// ExitPropertyKeyValuePairList is called when exiting the propertyKeyValuePairList production.
	ExitPropertyKeyValuePairList(c *PropertyKeyValuePairListContext)

	// ExitPropertyKeyValuePair is called when exiting the propertyKeyValuePair production.
	ExitPropertyKeyValuePair(c *PropertyKeyValuePairContext)

	// ExitEdgePattern is called when exiting the edgePattern production.
	ExitEdgePattern(c *EdgePatternContext)

	// ExitFullEdgePattern is called when exiting the fullEdgePattern production.
	ExitFullEdgePattern(c *FullEdgePatternContext)

	// ExitFullEdgePointingLeft is called when exiting the fullEdgePointingLeft production.
	ExitFullEdgePointingLeft(c *FullEdgePointingLeftContext)

	// ExitFullEdgeUndirected is called when exiting the fullEdgeUndirected production.
	ExitFullEdgeUndirected(c *FullEdgeUndirectedContext)

	// ExitFullEdgePointingRight is called when exiting the fullEdgePointingRight production.
	ExitFullEdgePointingRight(c *FullEdgePointingRightContext)

	// ExitFullEdgeLeftOrUndirected is called when exiting the fullEdgeLeftOrUndirected production.
	ExitFullEdgeLeftOrUndirected(c *FullEdgeLeftOrUndirectedContext)

	// ExitFullEdgeUndirectedOrRight is called when exiting the fullEdgeUndirectedOrRight production.
	ExitFullEdgeUndirectedOrRight(c *FullEdgeUndirectedOrRightContext)

	// ExitFullEdgeLeftOrRight is called when exiting the fullEdgeLeftOrRight production.
	ExitFullEdgeLeftOrRight(c *FullEdgeLeftOrRightContext)

	// ExitFullEdgeAnyDirection is called when exiting the fullEdgeAnyDirection production.
	ExitFullEdgeAnyDirection(c *FullEdgeAnyDirectionContext)

	// ExitAbbreviatedEdgePattern is called when exiting the abbreviatedEdgePattern production.
	ExitAbbreviatedEdgePattern(c *AbbreviatedEdgePatternContext)

	// ExitParenthesizedPathPatternExpression is called when exiting the parenthesizedPathPatternExpression production.
	ExitParenthesizedPathPatternExpression(c *ParenthesizedPathPatternExpressionContext)

	// ExitSubpathVariableDeclaration is called when exiting the subpathVariableDeclaration production.
	ExitSubpathVariableDeclaration(c *SubpathVariableDeclarationContext)

	// ExitParenthesizedPathPatternWhereClause is called when exiting the parenthesizedPathPatternWhereClause production.
	ExitParenthesizedPathPatternWhereClause(c *ParenthesizedPathPatternWhereClauseContext)

	// ExitLabelExpressionNegation is called when exiting the labelExpressionNegation production.
	ExitLabelExpressionNegation(c *LabelExpressionNegationContext)

	// ExitLabelExpressionDisjunction is called when exiting the labelExpressionDisjunction production.
	ExitLabelExpressionDisjunction(c *LabelExpressionDisjunctionContext)

	// ExitLabelExpressionParenthesized is called when exiting the labelExpressionParenthesized production.
	ExitLabelExpressionParenthesized(c *LabelExpressionParenthesizedContext)

	// ExitLabelExpressionWildcard is called when exiting the labelExpressionWildcard production.
	ExitLabelExpressionWildcard(c *LabelExpressionWildcardContext)

	// ExitLabelExpressionConjunction is called when exiting the labelExpressionConjunction production.
	ExitLabelExpressionConjunction(c *LabelExpressionConjunctionContext)

	// ExitLabelExpressionName is called when exiting the labelExpressionName production.
	ExitLabelExpressionName(c *LabelExpressionNameContext)

	// ExitPathVariableReference is called when exiting the pathVariableReference production.
	ExitPathVariableReference(c *PathVariableReferenceContext)

	// ExitElementVariableReference is called when exiting the elementVariableReference production.
	ExitElementVariableReference(c *ElementVariableReferenceContext)

	// ExitGraphPatternQuantifier is called when exiting the graphPatternQuantifier production.
	ExitGraphPatternQuantifier(c *GraphPatternQuantifierContext)

	// ExitFixedQuantifier is called when exiting the fixedQuantifier production.
	ExitFixedQuantifier(c *FixedQuantifierContext)

	// ExitGeneralQuantifier is called when exiting the generalQuantifier production.
	ExitGeneralQuantifier(c *GeneralQuantifierContext)

	// ExitLowerBound is called when exiting the lowerBound production.
	ExitLowerBound(c *LowerBoundContext)

	// ExitUpperBound is called when exiting the upperBound production.
	ExitUpperBound(c *UpperBoundContext)

	// ExitSimplifiedPathPatternExpression is called when exiting the simplifiedPathPatternExpression production.
	ExitSimplifiedPathPatternExpression(c *SimplifiedPathPatternExpressionContext)

	// ExitSimplifiedDefaultingLeft is called when exiting the simplifiedDefaultingLeft production.
	ExitSimplifiedDefaultingLeft(c *SimplifiedDefaultingLeftContext)

	// ExitSimplifiedDefaultingUndirected is called when exiting the simplifiedDefaultingUndirected production.
	ExitSimplifiedDefaultingUndirected(c *SimplifiedDefaultingUndirectedContext)

	// ExitSimplifiedDefaultingRight is called when exiting the simplifiedDefaultingRight production.
	ExitSimplifiedDefaultingRight(c *SimplifiedDefaultingRightContext)

	// ExitSimplifiedDefaultingLeftOrUndirected is called when exiting the simplifiedDefaultingLeftOrUndirected production.
	ExitSimplifiedDefaultingLeftOrUndirected(c *SimplifiedDefaultingLeftOrUndirectedContext)

	// ExitSimplifiedDefaultingUndirectedOrRight is called when exiting the simplifiedDefaultingUndirectedOrRight production.
	ExitSimplifiedDefaultingUndirectedOrRight(c *SimplifiedDefaultingUndirectedOrRightContext)

	// ExitSimplifiedDefaultingLeftOrRight is called when exiting the simplifiedDefaultingLeftOrRight production.
	ExitSimplifiedDefaultingLeftOrRight(c *SimplifiedDefaultingLeftOrRightContext)

	// ExitSimplifiedDefaultingAnyDirection is called when exiting the simplifiedDefaultingAnyDirection production.
	ExitSimplifiedDefaultingAnyDirection(c *SimplifiedDefaultingAnyDirectionContext)

	// ExitSimplifiedContents is called when exiting the simplifiedContents production.
	ExitSimplifiedContents(c *SimplifiedContentsContext)

	// ExitSimplifiedPathUnion is called when exiting the simplifiedPathUnion production.
	ExitSimplifiedPathUnion(c *SimplifiedPathUnionContext)

	// ExitSimplifiedMultisetAlternation is called when exiting the simplifiedMultisetAlternation production.
	ExitSimplifiedMultisetAlternation(c *SimplifiedMultisetAlternationContext)

	// ExitSimplifiedFactorLowLabel is called when exiting the simplifiedFactorLowLabel production.
	ExitSimplifiedFactorLowLabel(c *SimplifiedFactorLowLabelContext)

	// ExitSimplifiedConcatenationLabel is called when exiting the simplifiedConcatenationLabel production.
	ExitSimplifiedConcatenationLabel(c *SimplifiedConcatenationLabelContext)

	// ExitSimplifiedConjunctionLabel is called when exiting the simplifiedConjunctionLabel production.
	ExitSimplifiedConjunctionLabel(c *SimplifiedConjunctionLabelContext)

	// ExitSimplifiedFactorHighLabel is called when exiting the simplifiedFactorHighLabel production.
	ExitSimplifiedFactorHighLabel(c *SimplifiedFactorHighLabelContext)

	// ExitSimplifiedFactorHigh is called when exiting the simplifiedFactorHigh production.
	ExitSimplifiedFactorHigh(c *SimplifiedFactorHighContext)

	// ExitSimplifiedQuantified is called when exiting the simplifiedQuantified production.
	ExitSimplifiedQuantified(c *SimplifiedQuantifiedContext)

	// ExitSimplifiedQuestioned is called when exiting the simplifiedQuestioned production.
	ExitSimplifiedQuestioned(c *SimplifiedQuestionedContext)

	// ExitSimplifiedTertiary is called when exiting the simplifiedTertiary production.
	ExitSimplifiedTertiary(c *SimplifiedTertiaryContext)

	// ExitSimplifiedDirectionOverride is called when exiting the simplifiedDirectionOverride production.
	ExitSimplifiedDirectionOverride(c *SimplifiedDirectionOverrideContext)

	// ExitSimplifiedOverrideLeft is called when exiting the simplifiedOverrideLeft production.
	ExitSimplifiedOverrideLeft(c *SimplifiedOverrideLeftContext)

	// ExitSimplifiedOverrideUndirected is called when exiting the simplifiedOverrideUndirected production.
	ExitSimplifiedOverrideUndirected(c *SimplifiedOverrideUndirectedContext)

	// ExitSimplifiedOverrideRight is called when exiting the simplifiedOverrideRight production.
	ExitSimplifiedOverrideRight(c *SimplifiedOverrideRightContext)

	// ExitSimplifiedOverrideLeftOrUndirected is called when exiting the simplifiedOverrideLeftOrUndirected production.
	ExitSimplifiedOverrideLeftOrUndirected(c *SimplifiedOverrideLeftOrUndirectedContext)

	// ExitSimplifiedOverrideUndirectedOrRight is called when exiting the simplifiedOverrideUndirectedOrRight production.
	ExitSimplifiedOverrideUndirectedOrRight(c *SimplifiedOverrideUndirectedOrRightContext)

	// ExitSimplifiedOverrideLeftOrRight is called when exiting the simplifiedOverrideLeftOrRight production.
	ExitSimplifiedOverrideLeftOrRight(c *SimplifiedOverrideLeftOrRightContext)

	// ExitSimplifiedOverrideAnyDirection is called when exiting the simplifiedOverrideAnyDirection production.
	ExitSimplifiedOverrideAnyDirection(c *SimplifiedOverrideAnyDirectionContext)

	// ExitSimplifiedSecondary is called when exiting the simplifiedSecondary production.
	ExitSimplifiedSecondary(c *SimplifiedSecondaryContext)

	// ExitSimplifiedNegation is called when exiting the simplifiedNegation production.
	ExitSimplifiedNegation(c *SimplifiedNegationContext)

	// ExitSimplifiedPrimary is called when exiting the simplifiedPrimary production.
	ExitSimplifiedPrimary(c *SimplifiedPrimaryContext)

	// ExitWhereClause is called when exiting the whereClause production.
	ExitWhereClause(c *WhereClauseContext)

	// ExitYieldClause is called when exiting the yieldClause production.
	ExitYieldClause(c *YieldClauseContext)

	// ExitYieldItemList is called when exiting the yieldItemList production.
	ExitYieldItemList(c *YieldItemListContext)

	// ExitYieldItem is called when exiting the yieldItem production.
	ExitYieldItem(c *YieldItemContext)

	// ExitYieldItemName is called when exiting the yieldItemName production.
	ExitYieldItemName(c *YieldItemNameContext)

	// ExitYieldItemAlias is called when exiting the yieldItemAlias production.
	ExitYieldItemAlias(c *YieldItemAliasContext)

	// ExitGroupByClause is called when exiting the groupByClause production.
	ExitGroupByClause(c *GroupByClauseContext)

	// ExitGroupingElementList is called when exiting the groupingElementList production.
	ExitGroupingElementList(c *GroupingElementListContext)

	// ExitGroupingElement is called when exiting the groupingElement production.
	ExitGroupingElement(c *GroupingElementContext)

	// ExitEmptyGroupingSet is called when exiting the emptyGroupingSet production.
	ExitEmptyGroupingSet(c *EmptyGroupingSetContext)

	// ExitOrderByClause is called when exiting the orderByClause production.
	ExitOrderByClause(c *OrderByClauseContext)

	// ExitSortSpecificationList is called when exiting the sortSpecificationList production.
	ExitSortSpecificationList(c *SortSpecificationListContext)

	// ExitSortSpecification is called when exiting the sortSpecification production.
	ExitSortSpecification(c *SortSpecificationContext)

	// ExitSortKey is called when exiting the sortKey production.
	ExitSortKey(c *SortKeyContext)

	// ExitOrderingSpecification is called when exiting the orderingSpecification production.
	ExitOrderingSpecification(c *OrderingSpecificationContext)

	// ExitNullOrdering is called when exiting the nullOrdering production.
	ExitNullOrdering(c *NullOrderingContext)

	// ExitLimitClause is called when exiting the limitClause production.
	ExitLimitClause(c *LimitClauseContext)

	// ExitOffsetClause is called when exiting the offsetClause production.
	ExitOffsetClause(c *OffsetClauseContext)

	// ExitOffsetSynonym is called when exiting the offsetSynonym production.
	ExitOffsetSynonym(c *OffsetSynonymContext)

	// ExitSchemaReference is called when exiting the schemaReference production.
	ExitSchemaReference(c *SchemaReferenceContext)

	// ExitAbsoluteCatalogSchemaReference is called when exiting the absoluteCatalogSchemaReference production.
	ExitAbsoluteCatalogSchemaReference(c *AbsoluteCatalogSchemaReferenceContext)

	// ExitCatalogSchemaParentAndName is called when exiting the catalogSchemaParentAndName production.
	ExitCatalogSchemaParentAndName(c *CatalogSchemaParentAndNameContext)

	// ExitRelativeCatalogSchemaReference is called when exiting the relativeCatalogSchemaReference production.
	ExitRelativeCatalogSchemaReference(c *RelativeCatalogSchemaReferenceContext)

	// ExitPredefinedSchemaReference is called when exiting the predefinedSchemaReference production.
	ExitPredefinedSchemaReference(c *PredefinedSchemaReferenceContext)

	// ExitAbsoluteDirectoryPath is called when exiting the absoluteDirectoryPath production.
	ExitAbsoluteDirectoryPath(c *AbsoluteDirectoryPathContext)

	// ExitRelativeDirectoryPath is called when exiting the relativeDirectoryPath production.
	ExitRelativeDirectoryPath(c *RelativeDirectoryPathContext)

	// ExitSimpleDirectoryPath is called when exiting the simpleDirectoryPath production.
	ExitSimpleDirectoryPath(c *SimpleDirectoryPathContext)

	// ExitGraphReference is called when exiting the graphReference production.
	ExitGraphReference(c *GraphReferenceContext)

	// ExitCatalogGraphParentAndName is called when exiting the catalogGraphParentAndName production.
	ExitCatalogGraphParentAndName(c *CatalogGraphParentAndNameContext)

	// ExitHomeGraph is called when exiting the homeGraph production.
	ExitHomeGraph(c *HomeGraphContext)

	// ExitGraphTypeReference is called when exiting the graphTypeReference production.
	ExitGraphTypeReference(c *GraphTypeReferenceContext)

	// ExitCatalogGraphTypeParentAndName is called when exiting the catalogGraphTypeParentAndName production.
	ExitCatalogGraphTypeParentAndName(c *CatalogGraphTypeParentAndNameContext)

	// ExitBindingTableReference is called when exiting the bindingTableReference production.
	ExitBindingTableReference(c *BindingTableReferenceContext)

	// ExitProcedureReference is called when exiting the procedureReference production.
	ExitProcedureReference(c *ProcedureReferenceContext)

	// ExitCatalogProcedureParentAndName is called when exiting the catalogProcedureParentAndName production.
	ExitCatalogProcedureParentAndName(c *CatalogProcedureParentAndNameContext)

	// ExitCatalogObjectParentReference is called when exiting the catalogObjectParentReference production.
	ExitCatalogObjectParentReference(c *CatalogObjectParentReferenceContext)

	// ExitReferenceParameterSpecification is called when exiting the referenceParameterSpecification production.
	ExitReferenceParameterSpecification(c *ReferenceParameterSpecificationContext)

	// ExitNestedGraphTypeSpecification is called when exiting the nestedGraphTypeSpecification production.
	ExitNestedGraphTypeSpecification(c *NestedGraphTypeSpecificationContext)

	// ExitGraphTypeSpecificationBody is called when exiting the graphTypeSpecificationBody production.
	ExitGraphTypeSpecificationBody(c *GraphTypeSpecificationBodyContext)

	// ExitElementTypeList is called when exiting the elementTypeList production.
	ExitElementTypeList(c *ElementTypeListContext)

	// ExitElementTypeSpecification is called when exiting the elementTypeSpecification production.
	ExitElementTypeSpecification(c *ElementTypeSpecificationContext)

	// ExitNodeTypeSpecification is called when exiting the nodeTypeSpecification production.
	ExitNodeTypeSpecification(c *NodeTypeSpecificationContext)

	// ExitNodeTypePattern is called when exiting the nodeTypePattern production.
	ExitNodeTypePattern(c *NodeTypePatternContext)

	// ExitNodeTypePhrase is called when exiting the nodeTypePhrase production.
	ExitNodeTypePhrase(c *NodeTypePhraseContext)

	// ExitNodeTypePhraseFiller is called when exiting the nodeTypePhraseFiller production.
	ExitNodeTypePhraseFiller(c *NodeTypePhraseFillerContext)

	// ExitNodeTypeFiller is called when exiting the nodeTypeFiller production.
	ExitNodeTypeFiller(c *NodeTypeFillerContext)

	// ExitLocalNodeTypeAlias is called when exiting the localNodeTypeAlias production.
	ExitLocalNodeTypeAlias(c *LocalNodeTypeAliasContext)

	// ExitNodeTypeImpliedContent is called when exiting the nodeTypeImpliedContent production.
	ExitNodeTypeImpliedContent(c *NodeTypeImpliedContentContext)

	// ExitNodeTypeKeyLabelSet is called when exiting the nodeTypeKeyLabelSet production.
	ExitNodeTypeKeyLabelSet(c *NodeTypeKeyLabelSetContext)

	// ExitNodeTypeLabelSet is called when exiting the nodeTypeLabelSet production.
	ExitNodeTypeLabelSet(c *NodeTypeLabelSetContext)

	// ExitNodeTypePropertyTypes is called when exiting the nodeTypePropertyTypes production.
	ExitNodeTypePropertyTypes(c *NodeTypePropertyTypesContext)

	// ExitEdgeTypeSpecification is called when exiting the edgeTypeSpecification production.
	ExitEdgeTypeSpecification(c *EdgeTypeSpecificationContext)

	// ExitEdgeTypePattern is called when exiting the edgeTypePattern production.
	ExitEdgeTypePattern(c *EdgeTypePatternContext)

	// ExitEdgeTypePhrase is called when exiting the edgeTypePhrase production.
	ExitEdgeTypePhrase(c *EdgeTypePhraseContext)

	// ExitEdgeTypePhraseFiller is called when exiting the edgeTypePhraseFiller production.
	ExitEdgeTypePhraseFiller(c *EdgeTypePhraseFillerContext)

	// ExitEdgeTypeFiller is called when exiting the edgeTypeFiller production.
	ExitEdgeTypeFiller(c *EdgeTypeFillerContext)

	// ExitEdgeTypeImpliedContent is called when exiting the edgeTypeImpliedContent production.
	ExitEdgeTypeImpliedContent(c *EdgeTypeImpliedContentContext)

	// ExitEdgeTypeKeyLabelSet is called when exiting the edgeTypeKeyLabelSet production.
	ExitEdgeTypeKeyLabelSet(c *EdgeTypeKeyLabelSetContext)

	// ExitEdgeTypeLabelSet is called when exiting the edgeTypeLabelSet production.
	ExitEdgeTypeLabelSet(c *EdgeTypeLabelSetContext)

	// ExitEdgeTypePropertyTypes is called when exiting the edgeTypePropertyTypes production.
	ExitEdgeTypePropertyTypes(c *EdgeTypePropertyTypesContext)

	// ExitEdgeTypePatternDirected is called when exiting the edgeTypePatternDirected production.
	ExitEdgeTypePatternDirected(c *EdgeTypePatternDirectedContext)

	// ExitEdgeTypePatternPointingRight is called when exiting the edgeTypePatternPointingRight production.
	ExitEdgeTypePatternPointingRight(c *EdgeTypePatternPointingRightContext)

	// ExitEdgeTypePatternPointingLeft is called when exiting the edgeTypePatternPointingLeft production.
	ExitEdgeTypePatternPointingLeft(c *EdgeTypePatternPointingLeftContext)

	// ExitEdgeTypePatternUndirected is called when exiting the edgeTypePatternUndirected production.
	ExitEdgeTypePatternUndirected(c *EdgeTypePatternUndirectedContext)

	// ExitArcTypePointingRight is called when exiting the arcTypePointingRight production.
	ExitArcTypePointingRight(c *ArcTypePointingRightContext)

	// ExitArcTypePointingLeft is called when exiting the arcTypePointingLeft production.
	ExitArcTypePointingLeft(c *ArcTypePointingLeftContext)

	// ExitArcTypeUndirected is called when exiting the arcTypeUndirected production.
	ExitArcTypeUndirected(c *ArcTypeUndirectedContext)

	// ExitSourceNodeTypeReference is called when exiting the sourceNodeTypeReference production.
	ExitSourceNodeTypeReference(c *SourceNodeTypeReferenceContext)

	// ExitDestinationNodeTypeReference is called when exiting the destinationNodeTypeReference production.
	ExitDestinationNodeTypeReference(c *DestinationNodeTypeReferenceContext)

	// ExitEdgeKind is called when exiting the edgeKind production.
	ExitEdgeKind(c *EdgeKindContext)

	// ExitEndpointPairPhrase is called when exiting the endpointPairPhrase production.
	ExitEndpointPairPhrase(c *EndpointPairPhraseContext)

	// ExitEndpointPair is called when exiting the endpointPair production.
	ExitEndpointPair(c *EndpointPairContext)

	// ExitEndpointPairDirected is called when exiting the endpointPairDirected production.
	ExitEndpointPairDirected(c *EndpointPairDirectedContext)

	// ExitEndpointPairPointingRight is called when exiting the endpointPairPointingRight production.
	ExitEndpointPairPointingRight(c *EndpointPairPointingRightContext)

	// ExitEndpointPairPointingLeft is called when exiting the endpointPairPointingLeft production.
	ExitEndpointPairPointingLeft(c *EndpointPairPointingLeftContext)

	// ExitEndpointPairUndirected is called when exiting the endpointPairUndirected production.
	ExitEndpointPairUndirected(c *EndpointPairUndirectedContext)

	// ExitConnectorPointingRight is called when exiting the connectorPointingRight production.
	ExitConnectorPointingRight(c *ConnectorPointingRightContext)

	// ExitConnectorUndirected is called when exiting the connectorUndirected production.
	ExitConnectorUndirected(c *ConnectorUndirectedContext)

	// ExitSourceNodeTypeAlias is called when exiting the sourceNodeTypeAlias production.
	ExitSourceNodeTypeAlias(c *SourceNodeTypeAliasContext)

	// ExitDestinationNodeTypeAlias is called when exiting the destinationNodeTypeAlias production.
	ExitDestinationNodeTypeAlias(c *DestinationNodeTypeAliasContext)

	// ExitLabelSetPhrase is called when exiting the labelSetPhrase production.
	ExitLabelSetPhrase(c *LabelSetPhraseContext)

	// ExitLabelSetSpecification is called when exiting the labelSetSpecification production.
	ExitLabelSetSpecification(c *LabelSetSpecificationContext)

	// ExitPropertyTypesSpecification is called when exiting the propertyTypesSpecification production.
	ExitPropertyTypesSpecification(c *PropertyTypesSpecificationContext)

	// ExitPropertyTypeList is called when exiting the propertyTypeList production.
	ExitPropertyTypeList(c *PropertyTypeListContext)

	// ExitPropertyType is called when exiting the propertyType production.
	ExitPropertyType(c *PropertyTypeContext)

	// ExitPropertyValueType is called when exiting the propertyValueType production.
	ExitPropertyValueType(c *PropertyValueTypeContext)

	// ExitBindingTableType is called when exiting the bindingTableType production.
	ExitBindingTableType(c *BindingTableTypeContext)

	// ExitDynamicPropertyValueTypeLabel is called when exiting the dynamicPropertyValueTypeLabel production.
	ExitDynamicPropertyValueTypeLabel(c *DynamicPropertyValueTypeLabelContext)

	// ExitClosedDynamicUnionTypeAtl1 is called when exiting the closedDynamicUnionTypeAtl1 production.
	ExitClosedDynamicUnionTypeAtl1(c *ClosedDynamicUnionTypeAtl1Context)

	// ExitClosedDynamicUnionTypeAtl2 is called when exiting the closedDynamicUnionTypeAtl2 production.
	ExitClosedDynamicUnionTypeAtl2(c *ClosedDynamicUnionTypeAtl2Context)

	// ExitPathValueTypeLabel is called when exiting the pathValueTypeLabel production.
	ExitPathValueTypeLabel(c *PathValueTypeLabelContext)

	// ExitListValueTypeAlt3 is called when exiting the listValueTypeAlt3 production.
	ExitListValueTypeAlt3(c *ListValueTypeAlt3Context)

	// ExitListValueTypeAlt2 is called when exiting the listValueTypeAlt2 production.
	ExitListValueTypeAlt2(c *ListValueTypeAlt2Context)

	// ExitListValueTypeAlt1 is called when exiting the listValueTypeAlt1 production.
	ExitListValueTypeAlt1(c *ListValueTypeAlt1Context)

	// ExitPredefinedTypeLabel is called when exiting the predefinedTypeLabel production.
	ExitPredefinedTypeLabel(c *PredefinedTypeLabelContext)

	// ExitRecordTypeLabel is called when exiting the recordTypeLabel production.
	ExitRecordTypeLabel(c *RecordTypeLabelContext)

	// ExitOpenDynamicUnionTypeLabel is called when exiting the openDynamicUnionTypeLabel production.
	ExitOpenDynamicUnionTypeLabel(c *OpenDynamicUnionTypeLabelContext)

	// ExitTyped is called when exiting the typed production.
	ExitTyped(c *TypedContext)

	// ExitPredefinedType is called when exiting the predefinedType production.
	ExitPredefinedType(c *PredefinedTypeContext)

	// ExitBooleanType is called when exiting the booleanType production.
	ExitBooleanType(c *BooleanTypeContext)

	// ExitCharacterStringType is called when exiting the characterStringType production.
	ExitCharacterStringType(c *CharacterStringTypeContext)

	// ExitByteStringType is called when exiting the byteStringType production.
	ExitByteStringType(c *ByteStringTypeContext)

	// ExitMinLength is called when exiting the minLength production.
	ExitMinLength(c *MinLengthContext)

	// ExitMaxLength is called when exiting the maxLength production.
	ExitMaxLength(c *MaxLengthContext)

	// ExitFixedLength is called when exiting the fixedLength production.
	ExitFixedLength(c *FixedLengthContext)

	// ExitNumericType is called when exiting the numericType production.
	ExitNumericType(c *NumericTypeContext)

	// ExitExactNumericType is called when exiting the exactNumericType production.
	ExitExactNumericType(c *ExactNumericTypeContext)

	// ExitBinaryExactNumericType is called when exiting the binaryExactNumericType production.
	ExitBinaryExactNumericType(c *BinaryExactNumericTypeContext)

	// ExitSignedBinaryExactNumericType is called when exiting the signedBinaryExactNumericType production.
	ExitSignedBinaryExactNumericType(c *SignedBinaryExactNumericTypeContext)

	// ExitUnsignedBinaryExactNumericType is called when exiting the unsignedBinaryExactNumericType production.
	ExitUnsignedBinaryExactNumericType(c *UnsignedBinaryExactNumericTypeContext)

	// ExitVerboseBinaryExactNumericType is called when exiting the verboseBinaryExactNumericType production.
	ExitVerboseBinaryExactNumericType(c *VerboseBinaryExactNumericTypeContext)

	// ExitDecimalExactNumericType is called when exiting the decimalExactNumericType production.
	ExitDecimalExactNumericType(c *DecimalExactNumericTypeContext)

	// ExitPrecision is called when exiting the precision production.
	ExitPrecision(c *PrecisionContext)

	// ExitScale is called when exiting the scale production.
	ExitScale(c *ScaleContext)

	// ExitApproximateNumericType is called when exiting the approximateNumericType production.
	ExitApproximateNumericType(c *ApproximateNumericTypeContext)

	// ExitTemporalType is called when exiting the temporalType production.
	ExitTemporalType(c *TemporalTypeContext)

	// ExitTemporalInstantType is called when exiting the temporalInstantType production.
	ExitTemporalInstantType(c *TemporalInstantTypeContext)

	// ExitDatetimeType is called when exiting the datetimeType production.
	ExitDatetimeType(c *DatetimeTypeContext)

	// ExitLocaldatetimeType is called when exiting the localdatetimeType production.
	ExitLocaldatetimeType(c *LocaldatetimeTypeContext)

	// ExitDateType is called when exiting the dateType production.
	ExitDateType(c *DateTypeContext)

	// ExitTimeType is called when exiting the timeType production.
	ExitTimeType(c *TimeTypeContext)

	// ExitLocaltimeType is called when exiting the localtimeType production.
	ExitLocaltimeType(c *LocaltimeTypeContext)

	// ExitTemporalDurationType is called when exiting the temporalDurationType production.
	ExitTemporalDurationType(c *TemporalDurationTypeContext)

	// ExitTemporalDurationQualifier is called when exiting the temporalDurationQualifier production.
	ExitTemporalDurationQualifier(c *TemporalDurationQualifierContext)

	// ExitReferenceValueType is called when exiting the referenceValueType production.
	ExitReferenceValueType(c *ReferenceValueTypeContext)

	// ExitImmaterialValueType is called when exiting the immaterialValueType production.
	ExitImmaterialValueType(c *ImmaterialValueTypeContext)

	// ExitNullType is called when exiting the nullType production.
	ExitNullType(c *NullTypeContext)

	// ExitEmptyType is called when exiting the emptyType production.
	ExitEmptyType(c *EmptyTypeContext)

	// ExitGraphReferenceValueType is called when exiting the graphReferenceValueType production.
	ExitGraphReferenceValueType(c *GraphReferenceValueTypeContext)

	// ExitClosedGraphReferenceValueType is called when exiting the closedGraphReferenceValueType production.
	ExitClosedGraphReferenceValueType(c *ClosedGraphReferenceValueTypeContext)

	// ExitOpenGraphReferenceValueType is called when exiting the openGraphReferenceValueType production.
	ExitOpenGraphReferenceValueType(c *OpenGraphReferenceValueTypeContext)

	// ExitBindingTableReferenceValueType is called when exiting the bindingTableReferenceValueType production.
	ExitBindingTableReferenceValueType(c *BindingTableReferenceValueTypeContext)

	// ExitNodeReferenceValueType is called when exiting the nodeReferenceValueType production.
	ExitNodeReferenceValueType(c *NodeReferenceValueTypeContext)

	// ExitClosedNodeReferenceValueType is called when exiting the closedNodeReferenceValueType production.
	ExitClosedNodeReferenceValueType(c *ClosedNodeReferenceValueTypeContext)

	// ExitOpenNodeReferenceValueType is called when exiting the openNodeReferenceValueType production.
	ExitOpenNodeReferenceValueType(c *OpenNodeReferenceValueTypeContext)

	// ExitEdgeReferenceValueType is called when exiting the edgeReferenceValueType production.
	ExitEdgeReferenceValueType(c *EdgeReferenceValueTypeContext)

	// ExitClosedEdgeReferenceValueType is called when exiting the closedEdgeReferenceValueType production.
	ExitClosedEdgeReferenceValueType(c *ClosedEdgeReferenceValueTypeContext)

	// ExitOpenEdgeReferenceValueType is called when exiting the openEdgeReferenceValueType production.
	ExitOpenEdgeReferenceValueType(c *OpenEdgeReferenceValueTypeContext)

	// ExitPathValueType is called when exiting the pathValueType production.
	ExitPathValueType(c *PathValueTypeContext)

	// ExitListValueTypeName is called when exiting the listValueTypeName production.
	ExitListValueTypeName(c *ListValueTypeNameContext)

	// ExitListValueTypeNameSynonym is called when exiting the listValueTypeNameSynonym production.
	ExitListValueTypeNameSynonym(c *ListValueTypeNameSynonymContext)

	// ExitRecordType is called when exiting the recordType production.
	ExitRecordType(c *RecordTypeContext)

	// ExitFieldTypesSpecification is called when exiting the fieldTypesSpecification production.
	ExitFieldTypesSpecification(c *FieldTypesSpecificationContext)

	// ExitFieldTypeList is called when exiting the fieldTypeList production.
	ExitFieldTypeList(c *FieldTypeListContext)

	// ExitNotNull is called when exiting the notNull production.
	ExitNotNull(c *NotNullContext)

	// ExitFieldType is called when exiting the fieldType production.
	ExitFieldType(c *FieldTypeContext)

	// ExitSearchCondition is called when exiting the searchCondition production.
	ExitSearchCondition(c *SearchConditionContext)

	// ExitPredicate is called when exiting the predicate production.
	ExitPredicate(c *PredicateContext)

	// ExitCompOp is called when exiting the compOp production.
	ExitCompOp(c *CompOpContext)

	// ExitExistsPredicate is called when exiting the existsPredicate production.
	ExitExistsPredicate(c *ExistsPredicateContext)

	// ExitNullPredicate is called when exiting the nullPredicate production.
	ExitNullPredicate(c *NullPredicateContext)

	// ExitNullPredicatePart2 is called when exiting the nullPredicatePart2 production.
	ExitNullPredicatePart2(c *NullPredicatePart2Context)

	// ExitValueTypePredicate is called when exiting the valueTypePredicate production.
	ExitValueTypePredicate(c *ValueTypePredicateContext)

	// ExitValueTypePredicatePart2 is called when exiting the valueTypePredicatePart2 production.
	ExitValueTypePredicatePart2(c *ValueTypePredicatePart2Context)

	// ExitNormalizedPredicatePart2 is called when exiting the normalizedPredicatePart2 production.
	ExitNormalizedPredicatePart2(c *NormalizedPredicatePart2Context)

	// ExitDirectedPredicate is called when exiting the directedPredicate production.
	ExitDirectedPredicate(c *DirectedPredicateContext)

	// ExitDirectedPredicatePart2 is called when exiting the directedPredicatePart2 production.
	ExitDirectedPredicatePart2(c *DirectedPredicatePart2Context)

	// ExitLabeledPredicate is called when exiting the labeledPredicate production.
	ExitLabeledPredicate(c *LabeledPredicateContext)

	// ExitLabeledPredicatePart2 is called when exiting the labeledPredicatePart2 production.
	ExitLabeledPredicatePart2(c *LabeledPredicatePart2Context)

	// ExitIsLabeledOrColon is called when exiting the isLabeledOrColon production.
	ExitIsLabeledOrColon(c *IsLabeledOrColonContext)

	// ExitSourceDestinationPredicate is called when exiting the sourceDestinationPredicate production.
	ExitSourceDestinationPredicate(c *SourceDestinationPredicateContext)

	// ExitNodeReference is called when exiting the nodeReference production.
	ExitNodeReference(c *NodeReferenceContext)

	// ExitSourcePredicatePart2 is called when exiting the sourcePredicatePart2 production.
	ExitSourcePredicatePart2(c *SourcePredicatePart2Context)

	// ExitDestinationPredicatePart2 is called when exiting the destinationPredicatePart2 production.
	ExitDestinationPredicatePart2(c *DestinationPredicatePart2Context)

	// ExitEdgeReference is called when exiting the edgeReference production.
	ExitEdgeReference(c *EdgeReferenceContext)

	// ExitAll_differentPredicate is called when exiting the all_differentPredicate production.
	ExitAll_differentPredicate(c *All_differentPredicateContext)

	// ExitSamePredicate is called when exiting the samePredicate production.
	ExitSamePredicate(c *SamePredicateContext)

	// ExitProperty_existsPredicate is called when exiting the property_existsPredicate production.
	ExitProperty_existsPredicate(c *Property_existsPredicateContext)

	// ExitConjunctiveExprAlt is called when exiting the conjunctiveExprAlt production.
	ExitConjunctiveExprAlt(c *ConjunctiveExprAltContext)

	// ExitPropertyGraphExprAlt is called when exiting the propertyGraphExprAlt production.
	ExitPropertyGraphExprAlt(c *PropertyGraphExprAltContext)

	// ExitMultDivExprAlt is called when exiting the multDivExprAlt production.
	ExitMultDivExprAlt(c *MultDivExprAltContext)

	// ExitBindingTableExprAlt is called when exiting the bindingTableExprAlt production.
	ExitBindingTableExprAlt(c *BindingTableExprAltContext)

	// ExitSignedExprAlt is called when exiting the signedExprAlt production.
	ExitSignedExprAlt(c *SignedExprAltContext)

	// ExitIsNotExprAlt is called when exiting the isNotExprAlt production.
	ExitIsNotExprAlt(c *IsNotExprAltContext)

	// ExitNormalizedPredicateExprAlt is called when exiting the normalizedPredicateExprAlt production.
	ExitNormalizedPredicateExprAlt(c *NormalizedPredicateExprAltContext)

	// ExitNotExprAlt is called when exiting the notExprAlt production.
	ExitNotExprAlt(c *NotExprAltContext)

	// ExitValueFunctionExprAlt is called when exiting the valueFunctionExprAlt production.
	ExitValueFunctionExprAlt(c *ValueFunctionExprAltContext)

	// ExitConcatenationExprAlt is called when exiting the concatenationExprAlt production.
	ExitConcatenationExprAlt(c *ConcatenationExprAltContext)

	// ExitDisjunctiveExprAlt is called when exiting the disjunctiveExprAlt production.
	ExitDisjunctiveExprAlt(c *DisjunctiveExprAltContext)

	// ExitComparisonExprAlt is called when exiting the comparisonExprAlt production.
	ExitComparisonExprAlt(c *ComparisonExprAltContext)

	// ExitPrimaryExprAlt is called when exiting the primaryExprAlt production.
	ExitPrimaryExprAlt(c *PrimaryExprAltContext)

	// ExitAddSubtractExprAlt is called when exiting the addSubtractExprAlt production.
	ExitAddSubtractExprAlt(c *AddSubtractExprAltContext)

	// ExitPredicateExprAlt is called when exiting the predicateExprAlt production.
	ExitPredicateExprAlt(c *PredicateExprAltContext)

	// ExitValueFunction is called when exiting the valueFunction production.
	ExitValueFunction(c *ValueFunctionContext)

	// ExitBooleanValueExpression is called when exiting the booleanValueExpression production.
	ExitBooleanValueExpression(c *BooleanValueExpressionContext)

	// ExitCharacterOrByteStringFunction is called when exiting the characterOrByteStringFunction production.
	ExitCharacterOrByteStringFunction(c *CharacterOrByteStringFunctionContext)

	// ExitSubCharacterOrByteString is called when exiting the subCharacterOrByteString production.
	ExitSubCharacterOrByteString(c *SubCharacterOrByteStringContext)

	// ExitTrimSingleCharacterOrByteString is called when exiting the trimSingleCharacterOrByteString production.
	ExitTrimSingleCharacterOrByteString(c *TrimSingleCharacterOrByteStringContext)

	// ExitFoldCharacterString is called when exiting the foldCharacterString production.
	ExitFoldCharacterString(c *FoldCharacterStringContext)

	// ExitTrimMultiCharacterCharacterString is called when exiting the trimMultiCharacterCharacterString production.
	ExitTrimMultiCharacterCharacterString(c *TrimMultiCharacterCharacterStringContext)

	// ExitNormalizeCharacterString is called when exiting the normalizeCharacterString production.
	ExitNormalizeCharacterString(c *NormalizeCharacterStringContext)

	// ExitNodeReferenceValueExpression is called when exiting the nodeReferenceValueExpression production.
	ExitNodeReferenceValueExpression(c *NodeReferenceValueExpressionContext)

	// ExitEdgeReferenceValueExpression is called when exiting the edgeReferenceValueExpression production.
	ExitEdgeReferenceValueExpression(c *EdgeReferenceValueExpressionContext)

	// ExitAggregatingValueExpression is called when exiting the aggregatingValueExpression production.
	ExitAggregatingValueExpression(c *AggregatingValueExpressionContext)

	// ExitValueExpressionPrimary is called when exiting the valueExpressionPrimary production.
	ExitValueExpressionPrimary(c *ValueExpressionPrimaryContext)

	// ExitParenthesizedValueExpression is called when exiting the parenthesizedValueExpression production.
	ExitParenthesizedValueExpression(c *ParenthesizedValueExpressionContext)

	// ExitNonParenthesizedValueExpressionPrimary is called when exiting the nonParenthesizedValueExpressionPrimary production.
	ExitNonParenthesizedValueExpressionPrimary(c *NonParenthesizedValueExpressionPrimaryContext)

	// ExitNonParenthesizedValueExpressionPrimarySpecialCase is called when exiting the nonParenthesizedValueExpressionPrimarySpecialCase production.
	ExitNonParenthesizedValueExpressionPrimarySpecialCase(c *NonParenthesizedValueExpressionPrimarySpecialCaseContext)

	// ExitUnsignedValueSpecification is called when exiting the unsignedValueSpecification production.
	ExitUnsignedValueSpecification(c *UnsignedValueSpecificationContext)

	// ExitNonNegativeIntegerSpecification is called when exiting the nonNegativeIntegerSpecification production.
	ExitNonNegativeIntegerSpecification(c *NonNegativeIntegerSpecificationContext)

	// ExitGeneralValueSpecification is called when exiting the generalValueSpecification production.
	ExitGeneralValueSpecification(c *GeneralValueSpecificationContext)

	// ExitDynamicParameterSpecification is called when exiting the dynamicParameterSpecification production.
	ExitDynamicParameterSpecification(c *DynamicParameterSpecificationContext)

	// ExitLetValueExpression is called when exiting the letValueExpression production.
	ExitLetValueExpression(c *LetValueExpressionContext)

	// ExitValueQueryExpression is called when exiting the valueQueryExpression production.
	ExitValueQueryExpression(c *ValueQueryExpressionContext)

	// ExitCaseExpression is called when exiting the caseExpression production.
	ExitCaseExpression(c *CaseExpressionContext)

	// ExitCaseAbbreviation is called when exiting the caseAbbreviation production.
	ExitCaseAbbreviation(c *CaseAbbreviationContext)

	// ExitCaseSpecification is called when exiting the caseSpecification production.
	ExitCaseSpecification(c *CaseSpecificationContext)

	// ExitSimpleCase is called when exiting the simpleCase production.
	ExitSimpleCase(c *SimpleCaseContext)

	// ExitSearchedCase is called when exiting the searchedCase production.
	ExitSearchedCase(c *SearchedCaseContext)

	// ExitSimpleWhenClause is called when exiting the simpleWhenClause production.
	ExitSimpleWhenClause(c *SimpleWhenClauseContext)

	// ExitSearchedWhenClause is called when exiting the searchedWhenClause production.
	ExitSearchedWhenClause(c *SearchedWhenClauseContext)

	// ExitElseClause is called when exiting the elseClause production.
	ExitElseClause(c *ElseClauseContext)

	// ExitCaseOperand is called when exiting the caseOperand production.
	ExitCaseOperand(c *CaseOperandContext)

	// ExitWhenOperandList is called when exiting the whenOperandList production.
	ExitWhenOperandList(c *WhenOperandListContext)

	// ExitWhenOperand is called when exiting the whenOperand production.
	ExitWhenOperand(c *WhenOperandContext)

	// ExitResult is called when exiting the result production.
	ExitResult(c *ResultContext)

	// ExitResultExpression is called when exiting the resultExpression production.
	ExitResultExpression(c *ResultExpressionContext)

	// ExitCastSpecification is called when exiting the castSpecification production.
	ExitCastSpecification(c *CastSpecificationContext)

	// ExitCastOperand is called when exiting the castOperand production.
	ExitCastOperand(c *CastOperandContext)

	// ExitCastTarget is called when exiting the castTarget production.
	ExitCastTarget(c *CastTargetContext)

	// ExitAggregateFunction is called when exiting the aggregateFunction production.
	ExitAggregateFunction(c *AggregateFunctionContext)

	// ExitGeneralSetFunction is called when exiting the generalSetFunction production.
	ExitGeneralSetFunction(c *GeneralSetFunctionContext)

	// ExitBinarySetFunction is called when exiting the binarySetFunction production.
	ExitBinarySetFunction(c *BinarySetFunctionContext)

	// ExitGeneralSetFunctionType is called when exiting the generalSetFunctionType production.
	ExitGeneralSetFunctionType(c *GeneralSetFunctionTypeContext)

	// ExitSetQuantifier is called when exiting the setQuantifier production.
	ExitSetQuantifier(c *SetQuantifierContext)

	// ExitBinarySetFunctionType is called when exiting the binarySetFunctionType production.
	ExitBinarySetFunctionType(c *BinarySetFunctionTypeContext)

	// ExitDependentValueExpression is called when exiting the dependentValueExpression production.
	ExitDependentValueExpression(c *DependentValueExpressionContext)

	// ExitIndependentValueExpression is called when exiting the independentValueExpression production.
	ExitIndependentValueExpression(c *IndependentValueExpressionContext)

	// ExitElement_idFunction is called when exiting the element_idFunction production.
	ExitElement_idFunction(c *Element_idFunctionContext)

	// ExitBindingVariableReference is called when exiting the bindingVariableReference production.
	ExitBindingVariableReference(c *BindingVariableReferenceContext)

	// ExitPathValueExpression is called when exiting the pathValueExpression production.
	ExitPathValueExpression(c *PathValueExpressionContext)

	// ExitPathValueConstructor is called when exiting the pathValueConstructor production.
	ExitPathValueConstructor(c *PathValueConstructorContext)

	// ExitPathValueConstructorByEnumeration is called when exiting the pathValueConstructorByEnumeration production.
	ExitPathValueConstructorByEnumeration(c *PathValueConstructorByEnumerationContext)

	// ExitPathElementList is called when exiting the pathElementList production.
	ExitPathElementList(c *PathElementListContext)

	// ExitPathElementListStart is called when exiting the pathElementListStart production.
	ExitPathElementListStart(c *PathElementListStartContext)

	// ExitPathElementListStep is called when exiting the pathElementListStep production.
	ExitPathElementListStep(c *PathElementListStepContext)

	// ExitListValueExpression is called when exiting the listValueExpression production.
	ExitListValueExpression(c *ListValueExpressionContext)

	// ExitListValueFunction is called when exiting the listValueFunction production.
	ExitListValueFunction(c *ListValueFunctionContext)

	// ExitTrimListFunction is called when exiting the trimListFunction production.
	ExitTrimListFunction(c *TrimListFunctionContext)

	// ExitElementsFunction is called when exiting the elementsFunction production.
	ExitElementsFunction(c *ElementsFunctionContext)

	// ExitListValueConstructor is called when exiting the listValueConstructor production.
	ExitListValueConstructor(c *ListValueConstructorContext)

	// ExitListValueConstructorByEnumeration is called when exiting the listValueConstructorByEnumeration production.
	ExitListValueConstructorByEnumeration(c *ListValueConstructorByEnumerationContext)

	// ExitListElementList is called when exiting the listElementList production.
	ExitListElementList(c *ListElementListContext)

	// ExitListElement is called when exiting the listElement production.
	ExitListElement(c *ListElementContext)

	// ExitRecordConstructor is called when exiting the recordConstructor production.
	ExitRecordConstructor(c *RecordConstructorContext)

	// ExitFieldsSpecification is called when exiting the fieldsSpecification production.
	ExitFieldsSpecification(c *FieldsSpecificationContext)

	// ExitFieldList is called when exiting the fieldList production.
	ExitFieldList(c *FieldListContext)

	// ExitField is called when exiting the field production.
	ExitField(c *FieldContext)

	// ExitTruthValue is called when exiting the truthValue production.
	ExitTruthValue(c *TruthValueContext)

	// ExitNumericValueExpression is called when exiting the numericValueExpression production.
	ExitNumericValueExpression(c *NumericValueExpressionContext)

	// ExitNumericValueFunction is called when exiting the numericValueFunction production.
	ExitNumericValueFunction(c *NumericValueFunctionContext)

	// ExitLengthExpression is called when exiting the lengthExpression production.
	ExitLengthExpression(c *LengthExpressionContext)

	// ExitCardinalityExpression is called when exiting the cardinalityExpression production.
	ExitCardinalityExpression(c *CardinalityExpressionContext)

	// ExitCardinalityExpressionArgument is called when exiting the cardinalityExpressionArgument production.
	ExitCardinalityExpressionArgument(c *CardinalityExpressionArgumentContext)

	// ExitCharLengthExpression is called when exiting the charLengthExpression production.
	ExitCharLengthExpression(c *CharLengthExpressionContext)

	// ExitByteLengthExpression is called when exiting the byteLengthExpression production.
	ExitByteLengthExpression(c *ByteLengthExpressionContext)

	// ExitPathLengthExpression is called when exiting the pathLengthExpression production.
	ExitPathLengthExpression(c *PathLengthExpressionContext)

	// ExitAbsoluteValueExpression is called when exiting the absoluteValueExpression production.
	ExitAbsoluteValueExpression(c *AbsoluteValueExpressionContext)

	// ExitModulusExpression is called when exiting the modulusExpression production.
	ExitModulusExpression(c *ModulusExpressionContext)

	// ExitNumericValueExpressionDividend is called when exiting the numericValueExpressionDividend production.
	ExitNumericValueExpressionDividend(c *NumericValueExpressionDividendContext)

	// ExitNumericValueExpressionDivisor is called when exiting the numericValueExpressionDivisor production.
	ExitNumericValueExpressionDivisor(c *NumericValueExpressionDivisorContext)

	// ExitTrigonometricFunction is called when exiting the trigonometricFunction production.
	ExitTrigonometricFunction(c *TrigonometricFunctionContext)

	// ExitTrigonometricFunctionName is called when exiting the trigonometricFunctionName production.
	ExitTrigonometricFunctionName(c *TrigonometricFunctionNameContext)

	// ExitGeneralLogarithmFunction is called when exiting the generalLogarithmFunction production.
	ExitGeneralLogarithmFunction(c *GeneralLogarithmFunctionContext)

	// ExitGeneralLogarithmBase is called when exiting the generalLogarithmBase production.
	ExitGeneralLogarithmBase(c *GeneralLogarithmBaseContext)

	// ExitGeneralLogarithmArgument is called when exiting the generalLogarithmArgument production.
	ExitGeneralLogarithmArgument(c *GeneralLogarithmArgumentContext)

	// ExitCommonLogarithm is called when exiting the commonLogarithm production.
	ExitCommonLogarithm(c *CommonLogarithmContext)

	// ExitNaturalLogarithm is called when exiting the naturalLogarithm production.
	ExitNaturalLogarithm(c *NaturalLogarithmContext)

	// ExitExponentialFunction is called when exiting the exponentialFunction production.
	ExitExponentialFunction(c *ExponentialFunctionContext)

	// ExitPowerFunction is called when exiting the powerFunction production.
	ExitPowerFunction(c *PowerFunctionContext)

	// ExitNumericValueExpressionBase is called when exiting the numericValueExpressionBase production.
	ExitNumericValueExpressionBase(c *NumericValueExpressionBaseContext)

	// ExitNumericValueExpressionExponent is called when exiting the numericValueExpressionExponent production.
	ExitNumericValueExpressionExponent(c *NumericValueExpressionExponentContext)

	// ExitSquareRoot is called when exiting the squareRoot production.
	ExitSquareRoot(c *SquareRootContext)

	// ExitFloorFunction is called when exiting the floorFunction production.
	ExitFloorFunction(c *FloorFunctionContext)

	// ExitCeilingFunction is called when exiting the ceilingFunction production.
	ExitCeilingFunction(c *CeilingFunctionContext)

	// ExitCharacterStringValueExpression is called when exiting the characterStringValueExpression production.
	ExitCharacterStringValueExpression(c *CharacterStringValueExpressionContext)

	// ExitByteStringValueExpression is called when exiting the byteStringValueExpression production.
	ExitByteStringValueExpression(c *ByteStringValueExpressionContext)

	// ExitTrimOperands is called when exiting the trimOperands production.
	ExitTrimOperands(c *TrimOperandsContext)

	// ExitTrimCharacterOrByteStringSource is called when exiting the trimCharacterOrByteStringSource production.
	ExitTrimCharacterOrByteStringSource(c *TrimCharacterOrByteStringSourceContext)

	// ExitTrimSpecification is called when exiting the trimSpecification production.
	ExitTrimSpecification(c *TrimSpecificationContext)

	// ExitTrimCharacterOrByteString is called when exiting the trimCharacterOrByteString production.
	ExitTrimCharacterOrByteString(c *TrimCharacterOrByteStringContext)

	// ExitNormalForm is called when exiting the normalForm production.
	ExitNormalForm(c *NormalFormContext)

	// ExitStringLength is called when exiting the stringLength production.
	ExitStringLength(c *StringLengthContext)

	// ExitDatetimeValueExpression is called when exiting the datetimeValueExpression production.
	ExitDatetimeValueExpression(c *DatetimeValueExpressionContext)

	// ExitDatetimeValueFunction is called when exiting the datetimeValueFunction production.
	ExitDatetimeValueFunction(c *DatetimeValueFunctionContext)

	// ExitDateFunction is called when exiting the dateFunction production.
	ExitDateFunction(c *DateFunctionContext)

	// ExitTimeFunction is called when exiting the timeFunction production.
	ExitTimeFunction(c *TimeFunctionContext)

	// ExitLocaltimeFunction is called when exiting the localtimeFunction production.
	ExitLocaltimeFunction(c *LocaltimeFunctionContext)

	// ExitDatetimeFunction is called when exiting the datetimeFunction production.
	ExitDatetimeFunction(c *DatetimeFunctionContext)

	// ExitLocaldatetimeFunction is called when exiting the localdatetimeFunction production.
	ExitLocaldatetimeFunction(c *LocaldatetimeFunctionContext)

	// ExitDateFunctionParameters is called when exiting the dateFunctionParameters production.
	ExitDateFunctionParameters(c *DateFunctionParametersContext)

	// ExitTimeFunctionParameters is called when exiting the timeFunctionParameters production.
	ExitTimeFunctionParameters(c *TimeFunctionParametersContext)

	// ExitDatetimeFunctionParameters is called when exiting the datetimeFunctionParameters production.
	ExitDatetimeFunctionParameters(c *DatetimeFunctionParametersContext)

	// ExitDurationValueExpression is called when exiting the durationValueExpression production.
	ExitDurationValueExpression(c *DurationValueExpressionContext)

	// ExitDatetimeSubtraction is called when exiting the datetimeSubtraction production.
	ExitDatetimeSubtraction(c *DatetimeSubtractionContext)

	// ExitDatetimeSubtractionParameters is called when exiting the datetimeSubtractionParameters production.
	ExitDatetimeSubtractionParameters(c *DatetimeSubtractionParametersContext)

	// ExitDatetimeValueExpression1 is called when exiting the datetimeValueExpression1 production.
	ExitDatetimeValueExpression1(c *DatetimeValueExpression1Context)

	// ExitDatetimeValueExpression2 is called when exiting the datetimeValueExpression2 production.
	ExitDatetimeValueExpression2(c *DatetimeValueExpression2Context)

	// ExitDurationValueFunction is called when exiting the durationValueFunction production.
	ExitDurationValueFunction(c *DurationValueFunctionContext)

	// ExitDurationFunction is called when exiting the durationFunction production.
	ExitDurationFunction(c *DurationFunctionContext)

	// ExitDurationFunctionParameters is called when exiting the durationFunctionParameters production.
	ExitDurationFunctionParameters(c *DurationFunctionParametersContext)

	// ExitObjectName is called when exiting the objectName production.
	ExitObjectName(c *ObjectNameContext)

	// ExitObjectNameOrBindingVariable is called when exiting the objectNameOrBindingVariable production.
	ExitObjectNameOrBindingVariable(c *ObjectNameOrBindingVariableContext)

	// ExitDirectoryName is called when exiting the directoryName production.
	ExitDirectoryName(c *DirectoryNameContext)

	// ExitSchemaName is called when exiting the schemaName production.
	ExitSchemaName(c *SchemaNameContext)

	// ExitGraphName is called when exiting the graphName production.
	ExitGraphName(c *GraphNameContext)

	// ExitDelimitedGraphName is called when exiting the delimitedGraphName production.
	ExitDelimitedGraphName(c *DelimitedGraphNameContext)

	// ExitGraphTypeName is called when exiting the graphTypeName production.
	ExitGraphTypeName(c *GraphTypeNameContext)

	// ExitNodeTypeName is called when exiting the nodeTypeName production.
	ExitNodeTypeName(c *NodeTypeNameContext)

	// ExitEdgeTypeName is called when exiting the edgeTypeName production.
	ExitEdgeTypeName(c *EdgeTypeNameContext)

	// ExitBindingTableName is called when exiting the bindingTableName production.
	ExitBindingTableName(c *BindingTableNameContext)

	// ExitDelimitedBindingTableName is called when exiting the delimitedBindingTableName production.
	ExitDelimitedBindingTableName(c *DelimitedBindingTableNameContext)

	// ExitProcedureName is called when exiting the procedureName production.
	ExitProcedureName(c *ProcedureNameContext)

	// ExitLabelName is called when exiting the labelName production.
	ExitLabelName(c *LabelNameContext)

	// ExitPropertyName is called when exiting the propertyName production.
	ExitPropertyName(c *PropertyNameContext)

	// ExitFieldName is called when exiting the fieldName production.
	ExitFieldName(c *FieldNameContext)

	// ExitElementVariable is called when exiting the elementVariable production.
	ExitElementVariable(c *ElementVariableContext)

	// ExitPathVariable is called when exiting the pathVariable production.
	ExitPathVariable(c *PathVariableContext)

	// ExitSubpathVariable is called when exiting the subpathVariable production.
	ExitSubpathVariable(c *SubpathVariableContext)

	// ExitBindingVariable is called when exiting the bindingVariable production.
	ExitBindingVariable(c *BindingVariableContext)

	// ExitUnsignedLiteral is called when exiting the unsignedLiteral production.
	ExitUnsignedLiteral(c *UnsignedLiteralContext)

	// ExitGeneralLiteral is called when exiting the generalLiteral production.
	ExitGeneralLiteral(c *GeneralLiteralContext)

	// ExitTemporalLiteral is called when exiting the temporalLiteral production.
	ExitTemporalLiteral(c *TemporalLiteralContext)

	// ExitDateLiteral is called when exiting the dateLiteral production.
	ExitDateLiteral(c *DateLiteralContext)

	// ExitTimeLiteral is called when exiting the timeLiteral production.
	ExitTimeLiteral(c *TimeLiteralContext)

	// ExitDatetimeLiteral is called when exiting the datetimeLiteral production.
	ExitDatetimeLiteral(c *DatetimeLiteralContext)

	// ExitListLiteral is called when exiting the listLiteral production.
	ExitListLiteral(c *ListLiteralContext)

	// ExitRecordLiteral is called when exiting the recordLiteral production.
	ExitRecordLiteral(c *RecordLiteralContext)

	// ExitIdentifier is called when exiting the identifier production.
	ExitIdentifier(c *IdentifierContext)

	// ExitRegularIdentifier is called when exiting the regularIdentifier production.
	ExitRegularIdentifier(c *RegularIdentifierContext)

	// ExitTimeZoneString is called when exiting the timeZoneString production.
	ExitTimeZoneString(c *TimeZoneStringContext)

	// ExitCharacterStringLiteral is called when exiting the characterStringLiteral production.
	ExitCharacterStringLiteral(c *CharacterStringLiteralContext)

	// ExitUnsignedNumericLiteral is called when exiting the unsignedNumericLiteral production.
	ExitUnsignedNumericLiteral(c *UnsignedNumericLiteralContext)

	// ExitExactNumericLiteral is called when exiting the exactNumericLiteral production.
	ExitExactNumericLiteral(c *ExactNumericLiteralContext)

	// ExitApproximateNumericLiteral is called when exiting the approximateNumericLiteral production.
	ExitApproximateNumericLiteral(c *ApproximateNumericLiteralContext)

	// ExitUnsignedInteger is called when exiting the unsignedInteger production.
	ExitUnsignedInteger(c *UnsignedIntegerContext)

	// ExitUnsignedDecimalInteger is called when exiting the unsignedDecimalInteger production.
	ExitUnsignedDecimalInteger(c *UnsignedDecimalIntegerContext)

	// ExitNullLiteral is called when exiting the nullLiteral production.
	ExitNullLiteral(c *NullLiteralContext)

	// ExitDateString is called when exiting the dateString production.
	ExitDateString(c *DateStringContext)

	// ExitTimeString is called when exiting the timeString production.
	ExitTimeString(c *TimeStringContext)

	// ExitDatetimeString is called when exiting the datetimeString production.
	ExitDatetimeString(c *DatetimeStringContext)

	// ExitDurationLiteral is called when exiting the durationLiteral production.
	ExitDurationLiteral(c *DurationLiteralContext)

	// ExitDurationString is called when exiting the durationString production.
	ExitDurationString(c *DurationStringContext)

	// ExitNodeSynonym is called when exiting the nodeSynonym production.
	ExitNodeSynonym(c *NodeSynonymContext)

	// ExitEdgesSynonym is called when exiting the edgesSynonym production.
	ExitEdgesSynonym(c *EdgesSynonymContext)

	// ExitEdgeSynonym is called when exiting the edgeSynonym production.
	ExitEdgeSynonym(c *EdgeSynonymContext)

	// ExitNonReservedWords is called when exiting the nonReservedWords production.
	ExitNonReservedWords(c *NonReservedWordsContext)
}
