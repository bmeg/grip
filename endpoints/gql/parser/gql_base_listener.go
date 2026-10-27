// Code generated from GQL.g4 by ANTLR 4.13.2. DO NOT EDIT.

package parser // GQL

import "github.com/antlr4-go/antlr/v4"

// BaseGQLListener is a complete listener for a parse tree produced by GQLParser.
type BaseGQLListener struct{}

var _ GQLListener = &BaseGQLListener{}

// VisitTerminal is called when a terminal node is visited.
func (s *BaseGQLListener) VisitTerminal(node antlr.TerminalNode) {}

// VisitErrorNode is called when an error node is visited.
func (s *BaseGQLListener) VisitErrorNode(node antlr.ErrorNode) {}

// EnterEveryRule is called when any rule is entered.
func (s *BaseGQLListener) EnterEveryRule(ctx antlr.ParserRuleContext) {}

// ExitEveryRule is called when any rule is exited.
func (s *BaseGQLListener) ExitEveryRule(ctx antlr.ParserRuleContext) {}

// EnterGqlProgram is called when production gqlProgram is entered.
func (s *BaseGQLListener) EnterGqlProgram(ctx *GqlProgramContext) {}

// ExitGqlProgram is called when production gqlProgram is exited.
func (s *BaseGQLListener) ExitGqlProgram(ctx *GqlProgramContext) {}

// EnterProgramActivity is called when production programActivity is entered.
func (s *BaseGQLListener) EnterProgramActivity(ctx *ProgramActivityContext) {}

// ExitProgramActivity is called when production programActivity is exited.
func (s *BaseGQLListener) ExitProgramActivity(ctx *ProgramActivityContext) {}

// EnterSessionActivity is called when production sessionActivity is entered.
func (s *BaseGQLListener) EnterSessionActivity(ctx *SessionActivityContext) {}

// ExitSessionActivity is called when production sessionActivity is exited.
func (s *BaseGQLListener) ExitSessionActivity(ctx *SessionActivityContext) {}

// EnterTransactionActivity is called when production transactionActivity is entered.
func (s *BaseGQLListener) EnterTransactionActivity(ctx *TransactionActivityContext) {}

// ExitTransactionActivity is called when production transactionActivity is exited.
func (s *BaseGQLListener) ExitTransactionActivity(ctx *TransactionActivityContext) {}

// EnterEndTransactionCommand is called when production endTransactionCommand is entered.
func (s *BaseGQLListener) EnterEndTransactionCommand(ctx *EndTransactionCommandContext) {}

// ExitEndTransactionCommand is called when production endTransactionCommand is exited.
func (s *BaseGQLListener) ExitEndTransactionCommand(ctx *EndTransactionCommandContext) {}

// EnterSessionSetCommand is called when production sessionSetCommand is entered.
func (s *BaseGQLListener) EnterSessionSetCommand(ctx *SessionSetCommandContext) {}

// ExitSessionSetCommand is called when production sessionSetCommand is exited.
func (s *BaseGQLListener) ExitSessionSetCommand(ctx *SessionSetCommandContext) {}

// EnterSessionSetSchemaClause is called when production sessionSetSchemaClause is entered.
func (s *BaseGQLListener) EnterSessionSetSchemaClause(ctx *SessionSetSchemaClauseContext) {}

// ExitSessionSetSchemaClause is called when production sessionSetSchemaClause is exited.
func (s *BaseGQLListener) ExitSessionSetSchemaClause(ctx *SessionSetSchemaClauseContext) {}

// EnterSessionSetGraphClause is called when production sessionSetGraphClause is entered.
func (s *BaseGQLListener) EnterSessionSetGraphClause(ctx *SessionSetGraphClauseContext) {}

// ExitSessionSetGraphClause is called when production sessionSetGraphClause is exited.
func (s *BaseGQLListener) ExitSessionSetGraphClause(ctx *SessionSetGraphClauseContext) {}

// EnterSessionSetTimeZoneClause is called when production sessionSetTimeZoneClause is entered.
func (s *BaseGQLListener) EnterSessionSetTimeZoneClause(ctx *SessionSetTimeZoneClauseContext) {}

// ExitSessionSetTimeZoneClause is called when production sessionSetTimeZoneClause is exited.
func (s *BaseGQLListener) ExitSessionSetTimeZoneClause(ctx *SessionSetTimeZoneClauseContext) {}

// EnterSetTimeZoneValue is called when production setTimeZoneValue is entered.
func (s *BaseGQLListener) EnterSetTimeZoneValue(ctx *SetTimeZoneValueContext) {}

// ExitSetTimeZoneValue is called when production setTimeZoneValue is exited.
func (s *BaseGQLListener) ExitSetTimeZoneValue(ctx *SetTimeZoneValueContext) {}

// EnterSessionSetParameterClause is called when production sessionSetParameterClause is entered.
func (s *BaseGQLListener) EnterSessionSetParameterClause(ctx *SessionSetParameterClauseContext) {}

// ExitSessionSetParameterClause is called when production sessionSetParameterClause is exited.
func (s *BaseGQLListener) ExitSessionSetParameterClause(ctx *SessionSetParameterClauseContext) {}

// EnterSessionSetGraphParameterClause is called when production sessionSetGraphParameterClause is entered.
func (s *BaseGQLListener) EnterSessionSetGraphParameterClause(ctx *SessionSetGraphParameterClauseContext) {
}

// ExitSessionSetGraphParameterClause is called when production sessionSetGraphParameterClause is exited.
func (s *BaseGQLListener) ExitSessionSetGraphParameterClause(ctx *SessionSetGraphParameterClauseContext) {
}

// EnterSessionSetBindingTableParameterClause is called when production sessionSetBindingTableParameterClause is entered.
func (s *BaseGQLListener) EnterSessionSetBindingTableParameterClause(ctx *SessionSetBindingTableParameterClauseContext) {
}

// ExitSessionSetBindingTableParameterClause is called when production sessionSetBindingTableParameterClause is exited.
func (s *BaseGQLListener) ExitSessionSetBindingTableParameterClause(ctx *SessionSetBindingTableParameterClauseContext) {
}

// EnterSessionSetValueParameterClause is called when production sessionSetValueParameterClause is entered.
func (s *BaseGQLListener) EnterSessionSetValueParameterClause(ctx *SessionSetValueParameterClauseContext) {
}

// ExitSessionSetValueParameterClause is called when production sessionSetValueParameterClause is exited.
func (s *BaseGQLListener) ExitSessionSetValueParameterClause(ctx *SessionSetValueParameterClauseContext) {
}

// EnterSessionSetParameterName is called when production sessionSetParameterName is entered.
func (s *BaseGQLListener) EnterSessionSetParameterName(ctx *SessionSetParameterNameContext) {}

// ExitSessionSetParameterName is called when production sessionSetParameterName is exited.
func (s *BaseGQLListener) ExitSessionSetParameterName(ctx *SessionSetParameterNameContext) {}

// EnterSessionResetCommand is called when production sessionResetCommand is entered.
func (s *BaseGQLListener) EnterSessionResetCommand(ctx *SessionResetCommandContext) {}

// ExitSessionResetCommand is called when production sessionResetCommand is exited.
func (s *BaseGQLListener) ExitSessionResetCommand(ctx *SessionResetCommandContext) {}

// EnterSessionResetArguments is called when production sessionResetArguments is entered.
func (s *BaseGQLListener) EnterSessionResetArguments(ctx *SessionResetArgumentsContext) {}

// ExitSessionResetArguments is called when production sessionResetArguments is exited.
func (s *BaseGQLListener) ExitSessionResetArguments(ctx *SessionResetArgumentsContext) {}

// EnterSessionCloseCommand is called when production sessionCloseCommand is entered.
func (s *BaseGQLListener) EnterSessionCloseCommand(ctx *SessionCloseCommandContext) {}

// ExitSessionCloseCommand is called when production sessionCloseCommand is exited.
func (s *BaseGQLListener) ExitSessionCloseCommand(ctx *SessionCloseCommandContext) {}

// EnterSessionParameterSpecification is called when production sessionParameterSpecification is entered.
func (s *BaseGQLListener) EnterSessionParameterSpecification(ctx *SessionParameterSpecificationContext) {
}

// ExitSessionParameterSpecification is called when production sessionParameterSpecification is exited.
func (s *BaseGQLListener) ExitSessionParameterSpecification(ctx *SessionParameterSpecificationContext) {
}

// EnterStartTransactionCommand is called when production startTransactionCommand is entered.
func (s *BaseGQLListener) EnterStartTransactionCommand(ctx *StartTransactionCommandContext) {}

// ExitStartTransactionCommand is called when production startTransactionCommand is exited.
func (s *BaseGQLListener) ExitStartTransactionCommand(ctx *StartTransactionCommandContext) {}

// EnterTransactionCharacteristics is called when production transactionCharacteristics is entered.
func (s *BaseGQLListener) EnterTransactionCharacteristics(ctx *TransactionCharacteristicsContext) {}

// ExitTransactionCharacteristics is called when production transactionCharacteristics is exited.
func (s *BaseGQLListener) ExitTransactionCharacteristics(ctx *TransactionCharacteristicsContext) {}

// EnterTransactionMode is called when production transactionMode is entered.
func (s *BaseGQLListener) EnterTransactionMode(ctx *TransactionModeContext) {}

// ExitTransactionMode is called when production transactionMode is exited.
func (s *BaseGQLListener) ExitTransactionMode(ctx *TransactionModeContext) {}

// EnterTransactionAccessMode is called when production transactionAccessMode is entered.
func (s *BaseGQLListener) EnterTransactionAccessMode(ctx *TransactionAccessModeContext) {}

// ExitTransactionAccessMode is called when production transactionAccessMode is exited.
func (s *BaseGQLListener) ExitTransactionAccessMode(ctx *TransactionAccessModeContext) {}

// EnterRollbackCommand is called when production rollbackCommand is entered.
func (s *BaseGQLListener) EnterRollbackCommand(ctx *RollbackCommandContext) {}

// ExitRollbackCommand is called when production rollbackCommand is exited.
func (s *BaseGQLListener) ExitRollbackCommand(ctx *RollbackCommandContext) {}

// EnterCommitCommand is called when production commitCommand is entered.
func (s *BaseGQLListener) EnterCommitCommand(ctx *CommitCommandContext) {}

// ExitCommitCommand is called when production commitCommand is exited.
func (s *BaseGQLListener) ExitCommitCommand(ctx *CommitCommandContext) {}

// EnterNestedProcedureSpecification is called when production nestedProcedureSpecification is entered.
func (s *BaseGQLListener) EnterNestedProcedureSpecification(ctx *NestedProcedureSpecificationContext) {
}

// ExitNestedProcedureSpecification is called when production nestedProcedureSpecification is exited.
func (s *BaseGQLListener) ExitNestedProcedureSpecification(ctx *NestedProcedureSpecificationContext) {
}

// EnterProcedureSpecification is called when production procedureSpecification is entered.
func (s *BaseGQLListener) EnterProcedureSpecification(ctx *ProcedureSpecificationContext) {}

// ExitProcedureSpecification is called when production procedureSpecification is exited.
func (s *BaseGQLListener) ExitProcedureSpecification(ctx *ProcedureSpecificationContext) {}

// EnterNestedDataModifyingProcedureSpecification is called when production nestedDataModifyingProcedureSpecification is entered.
func (s *BaseGQLListener) EnterNestedDataModifyingProcedureSpecification(ctx *NestedDataModifyingProcedureSpecificationContext) {
}

// ExitNestedDataModifyingProcedureSpecification is called when production nestedDataModifyingProcedureSpecification is exited.
func (s *BaseGQLListener) ExitNestedDataModifyingProcedureSpecification(ctx *NestedDataModifyingProcedureSpecificationContext) {
}

// EnterNestedQuerySpecification is called when production nestedQuerySpecification is entered.
func (s *BaseGQLListener) EnterNestedQuerySpecification(ctx *NestedQuerySpecificationContext) {}

// ExitNestedQuerySpecification is called when production nestedQuerySpecification is exited.
func (s *BaseGQLListener) ExitNestedQuerySpecification(ctx *NestedQuerySpecificationContext) {}

// EnterProcedureBody is called when production procedureBody is entered.
func (s *BaseGQLListener) EnterProcedureBody(ctx *ProcedureBodyContext) {}

// ExitProcedureBody is called when production procedureBody is exited.
func (s *BaseGQLListener) ExitProcedureBody(ctx *ProcedureBodyContext) {}

// EnterBindingVariableDefinitionBlock is called when production bindingVariableDefinitionBlock is entered.
func (s *BaseGQLListener) EnterBindingVariableDefinitionBlock(ctx *BindingVariableDefinitionBlockContext) {
}

// ExitBindingVariableDefinitionBlock is called when production bindingVariableDefinitionBlock is exited.
func (s *BaseGQLListener) ExitBindingVariableDefinitionBlock(ctx *BindingVariableDefinitionBlockContext) {
}

// EnterBindingVariableDefinition is called when production bindingVariableDefinition is entered.
func (s *BaseGQLListener) EnterBindingVariableDefinition(ctx *BindingVariableDefinitionContext) {}

// ExitBindingVariableDefinition is called when production bindingVariableDefinition is exited.
func (s *BaseGQLListener) ExitBindingVariableDefinition(ctx *BindingVariableDefinitionContext) {}

// EnterStatementBlock is called when production statementBlock is entered.
func (s *BaseGQLListener) EnterStatementBlock(ctx *StatementBlockContext) {}

// ExitStatementBlock is called when production statementBlock is exited.
func (s *BaseGQLListener) ExitStatementBlock(ctx *StatementBlockContext) {}

// EnterStatement is called when production statement is entered.
func (s *BaseGQLListener) EnterStatement(ctx *StatementContext) {}

// ExitStatement is called when production statement is exited.
func (s *BaseGQLListener) ExitStatement(ctx *StatementContext) {}

// EnterNextStatement is called when production nextStatement is entered.
func (s *BaseGQLListener) EnterNextStatement(ctx *NextStatementContext) {}

// ExitNextStatement is called when production nextStatement is exited.
func (s *BaseGQLListener) ExitNextStatement(ctx *NextStatementContext) {}

// EnterGraphVariableDefinition is called when production graphVariableDefinition is entered.
func (s *BaseGQLListener) EnterGraphVariableDefinition(ctx *GraphVariableDefinitionContext) {}

// ExitGraphVariableDefinition is called when production graphVariableDefinition is exited.
func (s *BaseGQLListener) ExitGraphVariableDefinition(ctx *GraphVariableDefinitionContext) {}

// EnterOptTypedGraphInitializer is called when production optTypedGraphInitializer is entered.
func (s *BaseGQLListener) EnterOptTypedGraphInitializer(ctx *OptTypedGraphInitializerContext) {}

// ExitOptTypedGraphInitializer is called when production optTypedGraphInitializer is exited.
func (s *BaseGQLListener) ExitOptTypedGraphInitializer(ctx *OptTypedGraphInitializerContext) {}

// EnterGraphInitializer is called when production graphInitializer is entered.
func (s *BaseGQLListener) EnterGraphInitializer(ctx *GraphInitializerContext) {}

// ExitGraphInitializer is called when production graphInitializer is exited.
func (s *BaseGQLListener) ExitGraphInitializer(ctx *GraphInitializerContext) {}

// EnterBindingTableVariableDefinition is called when production bindingTableVariableDefinition is entered.
func (s *BaseGQLListener) EnterBindingTableVariableDefinition(ctx *BindingTableVariableDefinitionContext) {
}

// ExitBindingTableVariableDefinition is called when production bindingTableVariableDefinition is exited.
func (s *BaseGQLListener) ExitBindingTableVariableDefinition(ctx *BindingTableVariableDefinitionContext) {
}

// EnterOptTypedBindingTableInitializer is called when production optTypedBindingTableInitializer is entered.
func (s *BaseGQLListener) EnterOptTypedBindingTableInitializer(ctx *OptTypedBindingTableInitializerContext) {
}

// ExitOptTypedBindingTableInitializer is called when production optTypedBindingTableInitializer is exited.
func (s *BaseGQLListener) ExitOptTypedBindingTableInitializer(ctx *OptTypedBindingTableInitializerContext) {
}

// EnterBindingTableInitializer is called when production bindingTableInitializer is entered.
func (s *BaseGQLListener) EnterBindingTableInitializer(ctx *BindingTableInitializerContext) {}

// ExitBindingTableInitializer is called when production bindingTableInitializer is exited.
func (s *BaseGQLListener) ExitBindingTableInitializer(ctx *BindingTableInitializerContext) {}

// EnterValueVariableDefinition is called when production valueVariableDefinition is entered.
func (s *BaseGQLListener) EnterValueVariableDefinition(ctx *ValueVariableDefinitionContext) {}

// ExitValueVariableDefinition is called when production valueVariableDefinition is exited.
func (s *BaseGQLListener) ExitValueVariableDefinition(ctx *ValueVariableDefinitionContext) {}

// EnterOptTypedValueInitializer is called when production optTypedValueInitializer is entered.
func (s *BaseGQLListener) EnterOptTypedValueInitializer(ctx *OptTypedValueInitializerContext) {}

// ExitOptTypedValueInitializer is called when production optTypedValueInitializer is exited.
func (s *BaseGQLListener) ExitOptTypedValueInitializer(ctx *OptTypedValueInitializerContext) {}

// EnterValueInitializer is called when production valueInitializer is entered.
func (s *BaseGQLListener) EnterValueInitializer(ctx *ValueInitializerContext) {}

// ExitValueInitializer is called when production valueInitializer is exited.
func (s *BaseGQLListener) ExitValueInitializer(ctx *ValueInitializerContext) {}

// EnterGraphExpression is called when production graphExpression is entered.
func (s *BaseGQLListener) EnterGraphExpression(ctx *GraphExpressionContext) {}

// ExitGraphExpression is called when production graphExpression is exited.
func (s *BaseGQLListener) ExitGraphExpression(ctx *GraphExpressionContext) {}

// EnterCurrentGraph is called when production currentGraph is entered.
func (s *BaseGQLListener) EnterCurrentGraph(ctx *CurrentGraphContext) {}

// ExitCurrentGraph is called when production currentGraph is exited.
func (s *BaseGQLListener) ExitCurrentGraph(ctx *CurrentGraphContext) {}

// EnterBindingTableExpression is called when production bindingTableExpression is entered.
func (s *BaseGQLListener) EnterBindingTableExpression(ctx *BindingTableExpressionContext) {}

// ExitBindingTableExpression is called when production bindingTableExpression is exited.
func (s *BaseGQLListener) ExitBindingTableExpression(ctx *BindingTableExpressionContext) {}

// EnterNestedBindingTableQuerySpecification is called when production nestedBindingTableQuerySpecification is entered.
func (s *BaseGQLListener) EnterNestedBindingTableQuerySpecification(ctx *NestedBindingTableQuerySpecificationContext) {
}

// ExitNestedBindingTableQuerySpecification is called when production nestedBindingTableQuerySpecification is exited.
func (s *BaseGQLListener) ExitNestedBindingTableQuerySpecification(ctx *NestedBindingTableQuerySpecificationContext) {
}

// EnterObjectExpressionPrimary is called when production objectExpressionPrimary is entered.
func (s *BaseGQLListener) EnterObjectExpressionPrimary(ctx *ObjectExpressionPrimaryContext) {}

// ExitObjectExpressionPrimary is called when production objectExpressionPrimary is exited.
func (s *BaseGQLListener) ExitObjectExpressionPrimary(ctx *ObjectExpressionPrimaryContext) {}

// EnterLinearCatalogModifyingStatement is called when production linearCatalogModifyingStatement is entered.
func (s *BaseGQLListener) EnterLinearCatalogModifyingStatement(ctx *LinearCatalogModifyingStatementContext) {
}

// ExitLinearCatalogModifyingStatement is called when production linearCatalogModifyingStatement is exited.
func (s *BaseGQLListener) ExitLinearCatalogModifyingStatement(ctx *LinearCatalogModifyingStatementContext) {
}

// EnterSimpleCatalogModifyingStatement is called when production simpleCatalogModifyingStatement is entered.
func (s *BaseGQLListener) EnterSimpleCatalogModifyingStatement(ctx *SimpleCatalogModifyingStatementContext) {
}

// ExitSimpleCatalogModifyingStatement is called when production simpleCatalogModifyingStatement is exited.
func (s *BaseGQLListener) ExitSimpleCatalogModifyingStatement(ctx *SimpleCatalogModifyingStatementContext) {
}

// EnterPrimitiveCatalogModifyingStatement is called when production primitiveCatalogModifyingStatement is entered.
func (s *BaseGQLListener) EnterPrimitiveCatalogModifyingStatement(ctx *PrimitiveCatalogModifyingStatementContext) {
}

// ExitPrimitiveCatalogModifyingStatement is called when production primitiveCatalogModifyingStatement is exited.
func (s *BaseGQLListener) ExitPrimitiveCatalogModifyingStatement(ctx *PrimitiveCatalogModifyingStatementContext) {
}

// EnterCreateSchemaStatement is called when production createSchemaStatement is entered.
func (s *BaseGQLListener) EnterCreateSchemaStatement(ctx *CreateSchemaStatementContext) {}

// ExitCreateSchemaStatement is called when production createSchemaStatement is exited.
func (s *BaseGQLListener) ExitCreateSchemaStatement(ctx *CreateSchemaStatementContext) {}

// EnterDropSchemaStatement is called when production dropSchemaStatement is entered.
func (s *BaseGQLListener) EnterDropSchemaStatement(ctx *DropSchemaStatementContext) {}

// ExitDropSchemaStatement is called when production dropSchemaStatement is exited.
func (s *BaseGQLListener) ExitDropSchemaStatement(ctx *DropSchemaStatementContext) {}

// EnterCreateGraphStatement is called when production createGraphStatement is entered.
func (s *BaseGQLListener) EnterCreateGraphStatement(ctx *CreateGraphStatementContext) {}

// ExitCreateGraphStatement is called when production createGraphStatement is exited.
func (s *BaseGQLListener) ExitCreateGraphStatement(ctx *CreateGraphStatementContext) {}

// EnterOpenGraphType is called when production openGraphType is entered.
func (s *BaseGQLListener) EnterOpenGraphType(ctx *OpenGraphTypeContext) {}

// ExitOpenGraphType is called when production openGraphType is exited.
func (s *BaseGQLListener) ExitOpenGraphType(ctx *OpenGraphTypeContext) {}

// EnterOfGraphType is called when production ofGraphType is entered.
func (s *BaseGQLListener) EnterOfGraphType(ctx *OfGraphTypeContext) {}

// ExitOfGraphType is called when production ofGraphType is exited.
func (s *BaseGQLListener) ExitOfGraphType(ctx *OfGraphTypeContext) {}

// EnterGraphTypeLikeGraph is called when production graphTypeLikeGraph is entered.
func (s *BaseGQLListener) EnterGraphTypeLikeGraph(ctx *GraphTypeLikeGraphContext) {}

// ExitGraphTypeLikeGraph is called when production graphTypeLikeGraph is exited.
func (s *BaseGQLListener) ExitGraphTypeLikeGraph(ctx *GraphTypeLikeGraphContext) {}

// EnterGraphSource is called when production graphSource is entered.
func (s *BaseGQLListener) EnterGraphSource(ctx *GraphSourceContext) {}

// ExitGraphSource is called when production graphSource is exited.
func (s *BaseGQLListener) ExitGraphSource(ctx *GraphSourceContext) {}

// EnterDropGraphStatement is called when production dropGraphStatement is entered.
func (s *BaseGQLListener) EnterDropGraphStatement(ctx *DropGraphStatementContext) {}

// ExitDropGraphStatement is called when production dropGraphStatement is exited.
func (s *BaseGQLListener) ExitDropGraphStatement(ctx *DropGraphStatementContext) {}

// EnterCreateGraphTypeStatement is called when production createGraphTypeStatement is entered.
func (s *BaseGQLListener) EnterCreateGraphTypeStatement(ctx *CreateGraphTypeStatementContext) {}

// ExitCreateGraphTypeStatement is called when production createGraphTypeStatement is exited.
func (s *BaseGQLListener) ExitCreateGraphTypeStatement(ctx *CreateGraphTypeStatementContext) {}

// EnterGraphTypeSource is called when production graphTypeSource is entered.
func (s *BaseGQLListener) EnterGraphTypeSource(ctx *GraphTypeSourceContext) {}

// ExitGraphTypeSource is called when production graphTypeSource is exited.
func (s *BaseGQLListener) ExitGraphTypeSource(ctx *GraphTypeSourceContext) {}

// EnterCopyOfGraphType is called when production copyOfGraphType is entered.
func (s *BaseGQLListener) EnterCopyOfGraphType(ctx *CopyOfGraphTypeContext) {}

// ExitCopyOfGraphType is called when production copyOfGraphType is exited.
func (s *BaseGQLListener) ExitCopyOfGraphType(ctx *CopyOfGraphTypeContext) {}

// EnterDropGraphTypeStatement is called when production dropGraphTypeStatement is entered.
func (s *BaseGQLListener) EnterDropGraphTypeStatement(ctx *DropGraphTypeStatementContext) {}

// ExitDropGraphTypeStatement is called when production dropGraphTypeStatement is exited.
func (s *BaseGQLListener) ExitDropGraphTypeStatement(ctx *DropGraphTypeStatementContext) {}

// EnterCallCatalogModifyingProcedureStatement is called when production callCatalogModifyingProcedureStatement is entered.
func (s *BaseGQLListener) EnterCallCatalogModifyingProcedureStatement(ctx *CallCatalogModifyingProcedureStatementContext) {
}

// ExitCallCatalogModifyingProcedureStatement is called when production callCatalogModifyingProcedureStatement is exited.
func (s *BaseGQLListener) ExitCallCatalogModifyingProcedureStatement(ctx *CallCatalogModifyingProcedureStatementContext) {
}

// EnterLinearDataModifyingStatement is called when production linearDataModifyingStatement is entered.
func (s *BaseGQLListener) EnterLinearDataModifyingStatement(ctx *LinearDataModifyingStatementContext) {
}

// ExitLinearDataModifyingStatement is called when production linearDataModifyingStatement is exited.
func (s *BaseGQLListener) ExitLinearDataModifyingStatement(ctx *LinearDataModifyingStatementContext) {
}

// EnterFocusedLinearDataModifyingStatement is called when production focusedLinearDataModifyingStatement is entered.
func (s *BaseGQLListener) EnterFocusedLinearDataModifyingStatement(ctx *FocusedLinearDataModifyingStatementContext) {
}

// ExitFocusedLinearDataModifyingStatement is called when production focusedLinearDataModifyingStatement is exited.
func (s *BaseGQLListener) ExitFocusedLinearDataModifyingStatement(ctx *FocusedLinearDataModifyingStatementContext) {
}

// EnterFocusedLinearDataModifyingStatementBody is called when production focusedLinearDataModifyingStatementBody is entered.
func (s *BaseGQLListener) EnterFocusedLinearDataModifyingStatementBody(ctx *FocusedLinearDataModifyingStatementBodyContext) {
}

// ExitFocusedLinearDataModifyingStatementBody is called when production focusedLinearDataModifyingStatementBody is exited.
func (s *BaseGQLListener) ExitFocusedLinearDataModifyingStatementBody(ctx *FocusedLinearDataModifyingStatementBodyContext) {
}

// EnterFocusedNestedDataModifyingProcedureSpecification is called when production focusedNestedDataModifyingProcedureSpecification is entered.
func (s *BaseGQLListener) EnterFocusedNestedDataModifyingProcedureSpecification(ctx *FocusedNestedDataModifyingProcedureSpecificationContext) {
}

// ExitFocusedNestedDataModifyingProcedureSpecification is called when production focusedNestedDataModifyingProcedureSpecification is exited.
func (s *BaseGQLListener) ExitFocusedNestedDataModifyingProcedureSpecification(ctx *FocusedNestedDataModifyingProcedureSpecificationContext) {
}

// EnterAmbientLinearDataModifyingStatement is called when production ambientLinearDataModifyingStatement is entered.
func (s *BaseGQLListener) EnterAmbientLinearDataModifyingStatement(ctx *AmbientLinearDataModifyingStatementContext) {
}

// ExitAmbientLinearDataModifyingStatement is called when production ambientLinearDataModifyingStatement is exited.
func (s *BaseGQLListener) ExitAmbientLinearDataModifyingStatement(ctx *AmbientLinearDataModifyingStatementContext) {
}

// EnterAmbientLinearDataModifyingStatementBody is called when production ambientLinearDataModifyingStatementBody is entered.
func (s *BaseGQLListener) EnterAmbientLinearDataModifyingStatementBody(ctx *AmbientLinearDataModifyingStatementBodyContext) {
}

// ExitAmbientLinearDataModifyingStatementBody is called when production ambientLinearDataModifyingStatementBody is exited.
func (s *BaseGQLListener) ExitAmbientLinearDataModifyingStatementBody(ctx *AmbientLinearDataModifyingStatementBodyContext) {
}

// EnterSimpleLinearDataAccessingStatement is called when production simpleLinearDataAccessingStatement is entered.
func (s *BaseGQLListener) EnterSimpleLinearDataAccessingStatement(ctx *SimpleLinearDataAccessingStatementContext) {
}

// ExitSimpleLinearDataAccessingStatement is called when production simpleLinearDataAccessingStatement is exited.
func (s *BaseGQLListener) ExitSimpleLinearDataAccessingStatement(ctx *SimpleLinearDataAccessingStatementContext) {
}

// EnterSimpleDataAccessingStatement is called when production simpleDataAccessingStatement is entered.
func (s *BaseGQLListener) EnterSimpleDataAccessingStatement(ctx *SimpleDataAccessingStatementContext) {
}

// ExitSimpleDataAccessingStatement is called when production simpleDataAccessingStatement is exited.
func (s *BaseGQLListener) ExitSimpleDataAccessingStatement(ctx *SimpleDataAccessingStatementContext) {
}

// EnterSimpleDataModifyingStatement is called when production simpleDataModifyingStatement is entered.
func (s *BaseGQLListener) EnterSimpleDataModifyingStatement(ctx *SimpleDataModifyingStatementContext) {
}

// ExitSimpleDataModifyingStatement is called when production simpleDataModifyingStatement is exited.
func (s *BaseGQLListener) ExitSimpleDataModifyingStatement(ctx *SimpleDataModifyingStatementContext) {
}

// EnterPrimitiveDataModifyingStatement is called when production primitiveDataModifyingStatement is entered.
func (s *BaseGQLListener) EnterPrimitiveDataModifyingStatement(ctx *PrimitiveDataModifyingStatementContext) {
}

// ExitPrimitiveDataModifyingStatement is called when production primitiveDataModifyingStatement is exited.
func (s *BaseGQLListener) ExitPrimitiveDataModifyingStatement(ctx *PrimitiveDataModifyingStatementContext) {
}

// EnterInsertStatement is called when production insertStatement is entered.
func (s *BaseGQLListener) EnterInsertStatement(ctx *InsertStatementContext) {}

// ExitInsertStatement is called when production insertStatement is exited.
func (s *BaseGQLListener) ExitInsertStatement(ctx *InsertStatementContext) {}

// EnterSetStatement is called when production setStatement is entered.
func (s *BaseGQLListener) EnterSetStatement(ctx *SetStatementContext) {}

// ExitSetStatement is called when production setStatement is exited.
func (s *BaseGQLListener) ExitSetStatement(ctx *SetStatementContext) {}

// EnterSetItemList is called when production setItemList is entered.
func (s *BaseGQLListener) EnterSetItemList(ctx *SetItemListContext) {}

// ExitSetItemList is called when production setItemList is exited.
func (s *BaseGQLListener) ExitSetItemList(ctx *SetItemListContext) {}

// EnterSetItem is called when production setItem is entered.
func (s *BaseGQLListener) EnterSetItem(ctx *SetItemContext) {}

// ExitSetItem is called when production setItem is exited.
func (s *BaseGQLListener) ExitSetItem(ctx *SetItemContext) {}

// EnterSetPropertyItem is called when production setPropertyItem is entered.
func (s *BaseGQLListener) EnterSetPropertyItem(ctx *SetPropertyItemContext) {}

// ExitSetPropertyItem is called when production setPropertyItem is exited.
func (s *BaseGQLListener) ExitSetPropertyItem(ctx *SetPropertyItemContext) {}

// EnterSetAllPropertiesItem is called when production setAllPropertiesItem is entered.
func (s *BaseGQLListener) EnterSetAllPropertiesItem(ctx *SetAllPropertiesItemContext) {}

// ExitSetAllPropertiesItem is called when production setAllPropertiesItem is exited.
func (s *BaseGQLListener) ExitSetAllPropertiesItem(ctx *SetAllPropertiesItemContext) {}

// EnterSetLabelItem is called when production setLabelItem is entered.
func (s *BaseGQLListener) EnterSetLabelItem(ctx *SetLabelItemContext) {}

// ExitSetLabelItem is called when production setLabelItem is exited.
func (s *BaseGQLListener) ExitSetLabelItem(ctx *SetLabelItemContext) {}

// EnterRemoveStatement is called when production removeStatement is entered.
func (s *BaseGQLListener) EnterRemoveStatement(ctx *RemoveStatementContext) {}

// ExitRemoveStatement is called when production removeStatement is exited.
func (s *BaseGQLListener) ExitRemoveStatement(ctx *RemoveStatementContext) {}

// EnterRemoveItemList is called when production removeItemList is entered.
func (s *BaseGQLListener) EnterRemoveItemList(ctx *RemoveItemListContext) {}

// ExitRemoveItemList is called when production removeItemList is exited.
func (s *BaseGQLListener) ExitRemoveItemList(ctx *RemoveItemListContext) {}

// EnterRemoveItem is called when production removeItem is entered.
func (s *BaseGQLListener) EnterRemoveItem(ctx *RemoveItemContext) {}

// ExitRemoveItem is called when production removeItem is exited.
func (s *BaseGQLListener) ExitRemoveItem(ctx *RemoveItemContext) {}

// EnterRemovePropertyItem is called when production removePropertyItem is entered.
func (s *BaseGQLListener) EnterRemovePropertyItem(ctx *RemovePropertyItemContext) {}

// ExitRemovePropertyItem is called when production removePropertyItem is exited.
func (s *BaseGQLListener) ExitRemovePropertyItem(ctx *RemovePropertyItemContext) {}

// EnterRemoveLabelItem is called when production removeLabelItem is entered.
func (s *BaseGQLListener) EnterRemoveLabelItem(ctx *RemoveLabelItemContext) {}

// ExitRemoveLabelItem is called when production removeLabelItem is exited.
func (s *BaseGQLListener) ExitRemoveLabelItem(ctx *RemoveLabelItemContext) {}

// EnterDeleteStatement is called when production deleteStatement is entered.
func (s *BaseGQLListener) EnterDeleteStatement(ctx *DeleteStatementContext) {}

// ExitDeleteStatement is called when production deleteStatement is exited.
func (s *BaseGQLListener) ExitDeleteStatement(ctx *DeleteStatementContext) {}

// EnterDeleteItemList is called when production deleteItemList is entered.
func (s *BaseGQLListener) EnterDeleteItemList(ctx *DeleteItemListContext) {}

// ExitDeleteItemList is called when production deleteItemList is exited.
func (s *BaseGQLListener) ExitDeleteItemList(ctx *DeleteItemListContext) {}

// EnterDeleteItem is called when production deleteItem is entered.
func (s *BaseGQLListener) EnterDeleteItem(ctx *DeleteItemContext) {}

// ExitDeleteItem is called when production deleteItem is exited.
func (s *BaseGQLListener) ExitDeleteItem(ctx *DeleteItemContext) {}

// EnterCallDataModifyingProcedureStatement is called when production callDataModifyingProcedureStatement is entered.
func (s *BaseGQLListener) EnterCallDataModifyingProcedureStatement(ctx *CallDataModifyingProcedureStatementContext) {
}

// ExitCallDataModifyingProcedureStatement is called when production callDataModifyingProcedureStatement is exited.
func (s *BaseGQLListener) ExitCallDataModifyingProcedureStatement(ctx *CallDataModifyingProcedureStatementContext) {
}

// EnterCompositeQueryStatement is called when production compositeQueryStatement is entered.
func (s *BaseGQLListener) EnterCompositeQueryStatement(ctx *CompositeQueryStatementContext) {}

// ExitCompositeQueryStatement is called when production compositeQueryStatement is exited.
func (s *BaseGQLListener) ExitCompositeQueryStatement(ctx *CompositeQueryStatementContext) {}

// EnterCompositeQueryExpression is called when production compositeQueryExpression is entered.
func (s *BaseGQLListener) EnterCompositeQueryExpression(ctx *CompositeQueryExpressionContext) {}

// ExitCompositeQueryExpression is called when production compositeQueryExpression is exited.
func (s *BaseGQLListener) ExitCompositeQueryExpression(ctx *CompositeQueryExpressionContext) {}

// EnterQueryConjunction is called when production queryConjunction is entered.
func (s *BaseGQLListener) EnterQueryConjunction(ctx *QueryConjunctionContext) {}

// ExitQueryConjunction is called when production queryConjunction is exited.
func (s *BaseGQLListener) ExitQueryConjunction(ctx *QueryConjunctionContext) {}

// EnterSetOperator is called when production setOperator is entered.
func (s *BaseGQLListener) EnterSetOperator(ctx *SetOperatorContext) {}

// ExitSetOperator is called when production setOperator is exited.
func (s *BaseGQLListener) ExitSetOperator(ctx *SetOperatorContext) {}

// EnterCompositeQueryPrimary is called when production compositeQueryPrimary is entered.
func (s *BaseGQLListener) EnterCompositeQueryPrimary(ctx *CompositeQueryPrimaryContext) {}

// ExitCompositeQueryPrimary is called when production compositeQueryPrimary is exited.
func (s *BaseGQLListener) ExitCompositeQueryPrimary(ctx *CompositeQueryPrimaryContext) {}

// EnterLinearQueryStatement is called when production linearQueryStatement is entered.
func (s *BaseGQLListener) EnterLinearQueryStatement(ctx *LinearQueryStatementContext) {}

// ExitLinearQueryStatement is called when production linearQueryStatement is exited.
func (s *BaseGQLListener) ExitLinearQueryStatement(ctx *LinearQueryStatementContext) {}

// EnterFocusedLinearQueryStatement is called when production focusedLinearQueryStatement is entered.
func (s *BaseGQLListener) EnterFocusedLinearQueryStatement(ctx *FocusedLinearQueryStatementContext) {}

// ExitFocusedLinearQueryStatement is called when production focusedLinearQueryStatement is exited.
func (s *BaseGQLListener) ExitFocusedLinearQueryStatement(ctx *FocusedLinearQueryStatementContext) {}

// EnterFocusedLinearQueryStatementPart is called when production focusedLinearQueryStatementPart is entered.
func (s *BaseGQLListener) EnterFocusedLinearQueryStatementPart(ctx *FocusedLinearQueryStatementPartContext) {
}

// ExitFocusedLinearQueryStatementPart is called when production focusedLinearQueryStatementPart is exited.
func (s *BaseGQLListener) ExitFocusedLinearQueryStatementPart(ctx *FocusedLinearQueryStatementPartContext) {
}

// EnterFocusedLinearQueryAndPrimitiveResultStatementPart is called when production focusedLinearQueryAndPrimitiveResultStatementPart is entered.
func (s *BaseGQLListener) EnterFocusedLinearQueryAndPrimitiveResultStatementPart(ctx *FocusedLinearQueryAndPrimitiveResultStatementPartContext) {
}

// ExitFocusedLinearQueryAndPrimitiveResultStatementPart is called when production focusedLinearQueryAndPrimitiveResultStatementPart is exited.
func (s *BaseGQLListener) ExitFocusedLinearQueryAndPrimitiveResultStatementPart(ctx *FocusedLinearQueryAndPrimitiveResultStatementPartContext) {
}

// EnterFocusedPrimitiveResultStatement is called when production focusedPrimitiveResultStatement is entered.
func (s *BaseGQLListener) EnterFocusedPrimitiveResultStatement(ctx *FocusedPrimitiveResultStatementContext) {
}

// ExitFocusedPrimitiveResultStatement is called when production focusedPrimitiveResultStatement is exited.
func (s *BaseGQLListener) ExitFocusedPrimitiveResultStatement(ctx *FocusedPrimitiveResultStatementContext) {
}

// EnterFocusedNestedQuerySpecification is called when production focusedNestedQuerySpecification is entered.
func (s *BaseGQLListener) EnterFocusedNestedQuerySpecification(ctx *FocusedNestedQuerySpecificationContext) {
}

// ExitFocusedNestedQuerySpecification is called when production focusedNestedQuerySpecification is exited.
func (s *BaseGQLListener) ExitFocusedNestedQuerySpecification(ctx *FocusedNestedQuerySpecificationContext) {
}

// EnterAmbientLinearQueryStatement is called when production ambientLinearQueryStatement is entered.
func (s *BaseGQLListener) EnterAmbientLinearQueryStatement(ctx *AmbientLinearQueryStatementContext) {}

// ExitAmbientLinearQueryStatement is called when production ambientLinearQueryStatement is exited.
func (s *BaseGQLListener) ExitAmbientLinearQueryStatement(ctx *AmbientLinearQueryStatementContext) {}

// EnterSimpleLinearQueryStatement is called when production simpleLinearQueryStatement is entered.
func (s *BaseGQLListener) EnterSimpleLinearQueryStatement(ctx *SimpleLinearQueryStatementContext) {}

// ExitSimpleLinearQueryStatement is called when production simpleLinearQueryStatement is exited.
func (s *BaseGQLListener) ExitSimpleLinearQueryStatement(ctx *SimpleLinearQueryStatementContext) {}

// EnterSimpleQueryStatement is called when production simpleQueryStatement is entered.
func (s *BaseGQLListener) EnterSimpleQueryStatement(ctx *SimpleQueryStatementContext) {}

// ExitSimpleQueryStatement is called when production simpleQueryStatement is exited.
func (s *BaseGQLListener) ExitSimpleQueryStatement(ctx *SimpleQueryStatementContext) {}

// EnterPrimitiveQueryStatement is called when production primitiveQueryStatement is entered.
func (s *BaseGQLListener) EnterPrimitiveQueryStatement(ctx *PrimitiveQueryStatementContext) {}

// ExitPrimitiveQueryStatement is called when production primitiveQueryStatement is exited.
func (s *BaseGQLListener) ExitPrimitiveQueryStatement(ctx *PrimitiveQueryStatementContext) {}

// EnterMatchStatement is called when production matchStatement is entered.
func (s *BaseGQLListener) EnterMatchStatement(ctx *MatchStatementContext) {}

// ExitMatchStatement is called when production matchStatement is exited.
func (s *BaseGQLListener) ExitMatchStatement(ctx *MatchStatementContext) {}

// EnterSimpleMatchStatement is called when production simpleMatchStatement is entered.
func (s *BaseGQLListener) EnterSimpleMatchStatement(ctx *SimpleMatchStatementContext) {}

// ExitSimpleMatchStatement is called when production simpleMatchStatement is exited.
func (s *BaseGQLListener) ExitSimpleMatchStatement(ctx *SimpleMatchStatementContext) {}

// EnterOptionalMatchStatement is called when production optionalMatchStatement is entered.
func (s *BaseGQLListener) EnterOptionalMatchStatement(ctx *OptionalMatchStatementContext) {}

// ExitOptionalMatchStatement is called when production optionalMatchStatement is exited.
func (s *BaseGQLListener) ExitOptionalMatchStatement(ctx *OptionalMatchStatementContext) {}

// EnterOptionalOperand is called when production optionalOperand is entered.
func (s *BaseGQLListener) EnterOptionalOperand(ctx *OptionalOperandContext) {}

// ExitOptionalOperand is called when production optionalOperand is exited.
func (s *BaseGQLListener) ExitOptionalOperand(ctx *OptionalOperandContext) {}

// EnterMatchStatementBlock is called when production matchStatementBlock is entered.
func (s *BaseGQLListener) EnterMatchStatementBlock(ctx *MatchStatementBlockContext) {}

// ExitMatchStatementBlock is called when production matchStatementBlock is exited.
func (s *BaseGQLListener) ExitMatchStatementBlock(ctx *MatchStatementBlockContext) {}

// EnterCallQueryStatement is called when production callQueryStatement is entered.
func (s *BaseGQLListener) EnterCallQueryStatement(ctx *CallQueryStatementContext) {}

// ExitCallQueryStatement is called when production callQueryStatement is exited.
func (s *BaseGQLListener) ExitCallQueryStatement(ctx *CallQueryStatementContext) {}

// EnterFilterStatement is called when production filterStatement is entered.
func (s *BaseGQLListener) EnterFilterStatement(ctx *FilterStatementContext) {}

// ExitFilterStatement is called when production filterStatement is exited.
func (s *BaseGQLListener) ExitFilterStatement(ctx *FilterStatementContext) {}

// EnterLetStatement is called when production letStatement is entered.
func (s *BaseGQLListener) EnterLetStatement(ctx *LetStatementContext) {}

// ExitLetStatement is called when production letStatement is exited.
func (s *BaseGQLListener) ExitLetStatement(ctx *LetStatementContext) {}

// EnterLetVariableDefinitionList is called when production letVariableDefinitionList is entered.
func (s *BaseGQLListener) EnterLetVariableDefinitionList(ctx *LetVariableDefinitionListContext) {}

// ExitLetVariableDefinitionList is called when production letVariableDefinitionList is exited.
func (s *BaseGQLListener) ExitLetVariableDefinitionList(ctx *LetVariableDefinitionListContext) {}

// EnterLetVariableDefinition is called when production letVariableDefinition is entered.
func (s *BaseGQLListener) EnterLetVariableDefinition(ctx *LetVariableDefinitionContext) {}

// ExitLetVariableDefinition is called when production letVariableDefinition is exited.
func (s *BaseGQLListener) ExitLetVariableDefinition(ctx *LetVariableDefinitionContext) {}

// EnterForStatement is called when production forStatement is entered.
func (s *BaseGQLListener) EnterForStatement(ctx *ForStatementContext) {}

// ExitForStatement is called when production forStatement is exited.
func (s *BaseGQLListener) ExitForStatement(ctx *ForStatementContext) {}

// EnterForItem is called when production forItem is entered.
func (s *BaseGQLListener) EnterForItem(ctx *ForItemContext) {}

// ExitForItem is called when production forItem is exited.
func (s *BaseGQLListener) ExitForItem(ctx *ForItemContext) {}

// EnterForItemAlias is called when production forItemAlias is entered.
func (s *BaseGQLListener) EnterForItemAlias(ctx *ForItemAliasContext) {}

// ExitForItemAlias is called when production forItemAlias is exited.
func (s *BaseGQLListener) ExitForItemAlias(ctx *ForItemAliasContext) {}

// EnterForItemSource is called when production forItemSource is entered.
func (s *BaseGQLListener) EnterForItemSource(ctx *ForItemSourceContext) {}

// ExitForItemSource is called when production forItemSource is exited.
func (s *BaseGQLListener) ExitForItemSource(ctx *ForItemSourceContext) {}

// EnterForOrdinalityOrOffset is called when production forOrdinalityOrOffset is entered.
func (s *BaseGQLListener) EnterForOrdinalityOrOffset(ctx *ForOrdinalityOrOffsetContext) {}

// ExitForOrdinalityOrOffset is called when production forOrdinalityOrOffset is exited.
func (s *BaseGQLListener) ExitForOrdinalityOrOffset(ctx *ForOrdinalityOrOffsetContext) {}

// EnterOrderByAndPageStatement is called when production orderByAndPageStatement is entered.
func (s *BaseGQLListener) EnterOrderByAndPageStatement(ctx *OrderByAndPageStatementContext) {}

// ExitOrderByAndPageStatement is called when production orderByAndPageStatement is exited.
func (s *BaseGQLListener) ExitOrderByAndPageStatement(ctx *OrderByAndPageStatementContext) {}

// EnterPrimitiveResultStatement is called when production primitiveResultStatement is entered.
func (s *BaseGQLListener) EnterPrimitiveResultStatement(ctx *PrimitiveResultStatementContext) {}

// ExitPrimitiveResultStatement is called when production primitiveResultStatement is exited.
func (s *BaseGQLListener) ExitPrimitiveResultStatement(ctx *PrimitiveResultStatementContext) {}

// EnterReturnStatement is called when production returnStatement is entered.
func (s *BaseGQLListener) EnterReturnStatement(ctx *ReturnStatementContext) {}

// ExitReturnStatement is called when production returnStatement is exited.
func (s *BaseGQLListener) ExitReturnStatement(ctx *ReturnStatementContext) {}

// EnterReturnStatementBody is called when production returnStatementBody is entered.
func (s *BaseGQLListener) EnterReturnStatementBody(ctx *ReturnStatementBodyContext) {}

// ExitReturnStatementBody is called when production returnStatementBody is exited.
func (s *BaseGQLListener) ExitReturnStatementBody(ctx *ReturnStatementBodyContext) {}

// EnterReturnItemList is called when production returnItemList is entered.
func (s *BaseGQLListener) EnterReturnItemList(ctx *ReturnItemListContext) {}

// ExitReturnItemList is called when production returnItemList is exited.
func (s *BaseGQLListener) ExitReturnItemList(ctx *ReturnItemListContext) {}

// EnterReturnItem is called when production returnItem is entered.
func (s *BaseGQLListener) EnterReturnItem(ctx *ReturnItemContext) {}

// ExitReturnItem is called when production returnItem is exited.
func (s *BaseGQLListener) ExitReturnItem(ctx *ReturnItemContext) {}

// EnterReturnItemAlias is called when production returnItemAlias is entered.
func (s *BaseGQLListener) EnterReturnItemAlias(ctx *ReturnItemAliasContext) {}

// ExitReturnItemAlias is called when production returnItemAlias is exited.
func (s *BaseGQLListener) ExitReturnItemAlias(ctx *ReturnItemAliasContext) {}

// EnterSelectStatement is called when production selectStatement is entered.
func (s *BaseGQLListener) EnterSelectStatement(ctx *SelectStatementContext) {}

// ExitSelectStatement is called when production selectStatement is exited.
func (s *BaseGQLListener) ExitSelectStatement(ctx *SelectStatementContext) {}

// EnterSelectItemList is called when production selectItemList is entered.
func (s *BaseGQLListener) EnterSelectItemList(ctx *SelectItemListContext) {}

// ExitSelectItemList is called when production selectItemList is exited.
func (s *BaseGQLListener) ExitSelectItemList(ctx *SelectItemListContext) {}

// EnterSelectItem is called when production selectItem is entered.
func (s *BaseGQLListener) EnterSelectItem(ctx *SelectItemContext) {}

// ExitSelectItem is called when production selectItem is exited.
func (s *BaseGQLListener) ExitSelectItem(ctx *SelectItemContext) {}

// EnterSelectItemAlias is called when production selectItemAlias is entered.
func (s *BaseGQLListener) EnterSelectItemAlias(ctx *SelectItemAliasContext) {}

// ExitSelectItemAlias is called when production selectItemAlias is exited.
func (s *BaseGQLListener) ExitSelectItemAlias(ctx *SelectItemAliasContext) {}

// EnterHavingClause is called when production havingClause is entered.
func (s *BaseGQLListener) EnterHavingClause(ctx *HavingClauseContext) {}

// ExitHavingClause is called when production havingClause is exited.
func (s *BaseGQLListener) ExitHavingClause(ctx *HavingClauseContext) {}

// EnterSelectStatementBody is called when production selectStatementBody is entered.
func (s *BaseGQLListener) EnterSelectStatementBody(ctx *SelectStatementBodyContext) {}

// ExitSelectStatementBody is called when production selectStatementBody is exited.
func (s *BaseGQLListener) ExitSelectStatementBody(ctx *SelectStatementBodyContext) {}

// EnterSelectGraphMatchList is called when production selectGraphMatchList is entered.
func (s *BaseGQLListener) EnterSelectGraphMatchList(ctx *SelectGraphMatchListContext) {}

// ExitSelectGraphMatchList is called when production selectGraphMatchList is exited.
func (s *BaseGQLListener) ExitSelectGraphMatchList(ctx *SelectGraphMatchListContext) {}

// EnterSelectGraphMatch is called when production selectGraphMatch is entered.
func (s *BaseGQLListener) EnterSelectGraphMatch(ctx *SelectGraphMatchContext) {}

// ExitSelectGraphMatch is called when production selectGraphMatch is exited.
func (s *BaseGQLListener) ExitSelectGraphMatch(ctx *SelectGraphMatchContext) {}

// EnterSelectQuerySpecification is called when production selectQuerySpecification is entered.
func (s *BaseGQLListener) EnterSelectQuerySpecification(ctx *SelectQuerySpecificationContext) {}

// ExitSelectQuerySpecification is called when production selectQuerySpecification is exited.
func (s *BaseGQLListener) ExitSelectQuerySpecification(ctx *SelectQuerySpecificationContext) {}

// EnterCallProcedureStatement is called when production callProcedureStatement is entered.
func (s *BaseGQLListener) EnterCallProcedureStatement(ctx *CallProcedureStatementContext) {}

// ExitCallProcedureStatement is called when production callProcedureStatement is exited.
func (s *BaseGQLListener) ExitCallProcedureStatement(ctx *CallProcedureStatementContext) {}

// EnterProcedureCall is called when production procedureCall is entered.
func (s *BaseGQLListener) EnterProcedureCall(ctx *ProcedureCallContext) {}

// ExitProcedureCall is called when production procedureCall is exited.
func (s *BaseGQLListener) ExitProcedureCall(ctx *ProcedureCallContext) {}

// EnterInlineProcedureCall is called when production inlineProcedureCall is entered.
func (s *BaseGQLListener) EnterInlineProcedureCall(ctx *InlineProcedureCallContext) {}

// ExitInlineProcedureCall is called when production inlineProcedureCall is exited.
func (s *BaseGQLListener) ExitInlineProcedureCall(ctx *InlineProcedureCallContext) {}

// EnterVariableScopeClause is called when production variableScopeClause is entered.
func (s *BaseGQLListener) EnterVariableScopeClause(ctx *VariableScopeClauseContext) {}

// ExitVariableScopeClause is called when production variableScopeClause is exited.
func (s *BaseGQLListener) ExitVariableScopeClause(ctx *VariableScopeClauseContext) {}

// EnterBindingVariableReferenceList is called when production bindingVariableReferenceList is entered.
func (s *BaseGQLListener) EnterBindingVariableReferenceList(ctx *BindingVariableReferenceListContext) {
}

// ExitBindingVariableReferenceList is called when production bindingVariableReferenceList is exited.
func (s *BaseGQLListener) ExitBindingVariableReferenceList(ctx *BindingVariableReferenceListContext) {
}

// EnterNamedProcedureCall is called when production namedProcedureCall is entered.
func (s *BaseGQLListener) EnterNamedProcedureCall(ctx *NamedProcedureCallContext) {}

// ExitNamedProcedureCall is called when production namedProcedureCall is exited.
func (s *BaseGQLListener) ExitNamedProcedureCall(ctx *NamedProcedureCallContext) {}

// EnterProcedureArgumentList is called when production procedureArgumentList is entered.
func (s *BaseGQLListener) EnterProcedureArgumentList(ctx *ProcedureArgumentListContext) {}

// ExitProcedureArgumentList is called when production procedureArgumentList is exited.
func (s *BaseGQLListener) ExitProcedureArgumentList(ctx *ProcedureArgumentListContext) {}

// EnterProcedureArgument is called when production procedureArgument is entered.
func (s *BaseGQLListener) EnterProcedureArgument(ctx *ProcedureArgumentContext) {}

// ExitProcedureArgument is called when production procedureArgument is exited.
func (s *BaseGQLListener) ExitProcedureArgument(ctx *ProcedureArgumentContext) {}

// EnterAtSchemaClause is called when production atSchemaClause is entered.
func (s *BaseGQLListener) EnterAtSchemaClause(ctx *AtSchemaClauseContext) {}

// ExitAtSchemaClause is called when production atSchemaClause is exited.
func (s *BaseGQLListener) ExitAtSchemaClause(ctx *AtSchemaClauseContext) {}

// EnterUseGraphClause is called when production useGraphClause is entered.
func (s *BaseGQLListener) EnterUseGraphClause(ctx *UseGraphClauseContext) {}

// ExitUseGraphClause is called when production useGraphClause is exited.
func (s *BaseGQLListener) ExitUseGraphClause(ctx *UseGraphClauseContext) {}

// EnterGraphPatternBindingTable is called when production graphPatternBindingTable is entered.
func (s *BaseGQLListener) EnterGraphPatternBindingTable(ctx *GraphPatternBindingTableContext) {}

// ExitGraphPatternBindingTable is called when production graphPatternBindingTable is exited.
func (s *BaseGQLListener) ExitGraphPatternBindingTable(ctx *GraphPatternBindingTableContext) {}

// EnterGraphPatternYieldClause is called when production graphPatternYieldClause is entered.
func (s *BaseGQLListener) EnterGraphPatternYieldClause(ctx *GraphPatternYieldClauseContext) {}

// ExitGraphPatternYieldClause is called when production graphPatternYieldClause is exited.
func (s *BaseGQLListener) ExitGraphPatternYieldClause(ctx *GraphPatternYieldClauseContext) {}

// EnterGraphPatternYieldItemList is called when production graphPatternYieldItemList is entered.
func (s *BaseGQLListener) EnterGraphPatternYieldItemList(ctx *GraphPatternYieldItemListContext) {}

// ExitGraphPatternYieldItemList is called when production graphPatternYieldItemList is exited.
func (s *BaseGQLListener) ExitGraphPatternYieldItemList(ctx *GraphPatternYieldItemListContext) {}

// EnterGraphPatternYieldItem is called when production graphPatternYieldItem is entered.
func (s *BaseGQLListener) EnterGraphPatternYieldItem(ctx *GraphPatternYieldItemContext) {}

// ExitGraphPatternYieldItem is called when production graphPatternYieldItem is exited.
func (s *BaseGQLListener) ExitGraphPatternYieldItem(ctx *GraphPatternYieldItemContext) {}

// EnterGraphPattern is called when production graphPattern is entered.
func (s *BaseGQLListener) EnterGraphPattern(ctx *GraphPatternContext) {}

// ExitGraphPattern is called when production graphPattern is exited.
func (s *BaseGQLListener) ExitGraphPattern(ctx *GraphPatternContext) {}

// EnterMatchMode is called when production matchMode is entered.
func (s *BaseGQLListener) EnterMatchMode(ctx *MatchModeContext) {}

// ExitMatchMode is called when production matchMode is exited.
func (s *BaseGQLListener) ExitMatchMode(ctx *MatchModeContext) {}

// EnterRepeatableElementsMatchMode is called when production repeatableElementsMatchMode is entered.
func (s *BaseGQLListener) EnterRepeatableElementsMatchMode(ctx *RepeatableElementsMatchModeContext) {}

// ExitRepeatableElementsMatchMode is called when production repeatableElementsMatchMode is exited.
func (s *BaseGQLListener) ExitRepeatableElementsMatchMode(ctx *RepeatableElementsMatchModeContext) {}

// EnterDifferentEdgesMatchMode is called when production differentEdgesMatchMode is entered.
func (s *BaseGQLListener) EnterDifferentEdgesMatchMode(ctx *DifferentEdgesMatchModeContext) {}

// ExitDifferentEdgesMatchMode is called when production differentEdgesMatchMode is exited.
func (s *BaseGQLListener) ExitDifferentEdgesMatchMode(ctx *DifferentEdgesMatchModeContext) {}

// EnterElementBindingsOrElements is called when production elementBindingsOrElements is entered.
func (s *BaseGQLListener) EnterElementBindingsOrElements(ctx *ElementBindingsOrElementsContext) {}

// ExitElementBindingsOrElements is called when production elementBindingsOrElements is exited.
func (s *BaseGQLListener) ExitElementBindingsOrElements(ctx *ElementBindingsOrElementsContext) {}

// EnterEdgeBindingsOrEdges is called when production edgeBindingsOrEdges is entered.
func (s *BaseGQLListener) EnterEdgeBindingsOrEdges(ctx *EdgeBindingsOrEdgesContext) {}

// ExitEdgeBindingsOrEdges is called when production edgeBindingsOrEdges is exited.
func (s *BaseGQLListener) ExitEdgeBindingsOrEdges(ctx *EdgeBindingsOrEdgesContext) {}

// EnterPathPatternList is called when production pathPatternList is entered.
func (s *BaseGQLListener) EnterPathPatternList(ctx *PathPatternListContext) {}

// ExitPathPatternList is called when production pathPatternList is exited.
func (s *BaseGQLListener) ExitPathPatternList(ctx *PathPatternListContext) {}

// EnterPathPattern is called when production pathPattern is entered.
func (s *BaseGQLListener) EnterPathPattern(ctx *PathPatternContext) {}

// ExitPathPattern is called when production pathPattern is exited.
func (s *BaseGQLListener) ExitPathPattern(ctx *PathPatternContext) {}

// EnterPathVariableDeclaration is called when production pathVariableDeclaration is entered.
func (s *BaseGQLListener) EnterPathVariableDeclaration(ctx *PathVariableDeclarationContext) {}

// ExitPathVariableDeclaration is called when production pathVariableDeclaration is exited.
func (s *BaseGQLListener) ExitPathVariableDeclaration(ctx *PathVariableDeclarationContext) {}

// EnterKeepClause is called when production keepClause is entered.
func (s *BaseGQLListener) EnterKeepClause(ctx *KeepClauseContext) {}

// ExitKeepClause is called when production keepClause is exited.
func (s *BaseGQLListener) ExitKeepClause(ctx *KeepClauseContext) {}

// EnterGraphPatternWhereClause is called when production graphPatternWhereClause is entered.
func (s *BaseGQLListener) EnterGraphPatternWhereClause(ctx *GraphPatternWhereClauseContext) {}

// ExitGraphPatternWhereClause is called when production graphPatternWhereClause is exited.
func (s *BaseGQLListener) ExitGraphPatternWhereClause(ctx *GraphPatternWhereClauseContext) {}

// EnterInsertGraphPattern is called when production insertGraphPattern is entered.
func (s *BaseGQLListener) EnterInsertGraphPattern(ctx *InsertGraphPatternContext) {}

// ExitInsertGraphPattern is called when production insertGraphPattern is exited.
func (s *BaseGQLListener) ExitInsertGraphPattern(ctx *InsertGraphPatternContext) {}

// EnterInsertPathPatternList is called when production insertPathPatternList is entered.
func (s *BaseGQLListener) EnterInsertPathPatternList(ctx *InsertPathPatternListContext) {}

// ExitInsertPathPatternList is called when production insertPathPatternList is exited.
func (s *BaseGQLListener) ExitInsertPathPatternList(ctx *InsertPathPatternListContext) {}

// EnterInsertPathPattern is called when production insertPathPattern is entered.
func (s *BaseGQLListener) EnterInsertPathPattern(ctx *InsertPathPatternContext) {}

// ExitInsertPathPattern is called when production insertPathPattern is exited.
func (s *BaseGQLListener) ExitInsertPathPattern(ctx *InsertPathPatternContext) {}

// EnterInsertNodePattern is called when production insertNodePattern is entered.
func (s *BaseGQLListener) EnterInsertNodePattern(ctx *InsertNodePatternContext) {}

// ExitInsertNodePattern is called when production insertNodePattern is exited.
func (s *BaseGQLListener) ExitInsertNodePattern(ctx *InsertNodePatternContext) {}

// EnterInsertEdgePattern is called when production insertEdgePattern is entered.
func (s *BaseGQLListener) EnterInsertEdgePattern(ctx *InsertEdgePatternContext) {}

// ExitInsertEdgePattern is called when production insertEdgePattern is exited.
func (s *BaseGQLListener) ExitInsertEdgePattern(ctx *InsertEdgePatternContext) {}

// EnterInsertEdgePointingLeft is called when production insertEdgePointingLeft is entered.
func (s *BaseGQLListener) EnterInsertEdgePointingLeft(ctx *InsertEdgePointingLeftContext) {}

// ExitInsertEdgePointingLeft is called when production insertEdgePointingLeft is exited.
func (s *BaseGQLListener) ExitInsertEdgePointingLeft(ctx *InsertEdgePointingLeftContext) {}

// EnterInsertEdgePointingRight is called when production insertEdgePointingRight is entered.
func (s *BaseGQLListener) EnterInsertEdgePointingRight(ctx *InsertEdgePointingRightContext) {}

// ExitInsertEdgePointingRight is called when production insertEdgePointingRight is exited.
func (s *BaseGQLListener) ExitInsertEdgePointingRight(ctx *InsertEdgePointingRightContext) {}

// EnterInsertEdgeUndirected is called when production insertEdgeUndirected is entered.
func (s *BaseGQLListener) EnterInsertEdgeUndirected(ctx *InsertEdgeUndirectedContext) {}

// ExitInsertEdgeUndirected is called when production insertEdgeUndirected is exited.
func (s *BaseGQLListener) ExitInsertEdgeUndirected(ctx *InsertEdgeUndirectedContext) {}

// EnterInsertElementPatternFiller is called when production insertElementPatternFiller is entered.
func (s *BaseGQLListener) EnterInsertElementPatternFiller(ctx *InsertElementPatternFillerContext) {}

// ExitInsertElementPatternFiller is called when production insertElementPatternFiller is exited.
func (s *BaseGQLListener) ExitInsertElementPatternFiller(ctx *InsertElementPatternFillerContext) {}

// EnterLabelAndPropertySetSpecification is called when production labelAndPropertySetSpecification is entered.
func (s *BaseGQLListener) EnterLabelAndPropertySetSpecification(ctx *LabelAndPropertySetSpecificationContext) {
}

// ExitLabelAndPropertySetSpecification is called when production labelAndPropertySetSpecification is exited.
func (s *BaseGQLListener) ExitLabelAndPropertySetSpecification(ctx *LabelAndPropertySetSpecificationContext) {
}

// EnterPathPatternPrefix is called when production pathPatternPrefix is entered.
func (s *BaseGQLListener) EnterPathPatternPrefix(ctx *PathPatternPrefixContext) {}

// ExitPathPatternPrefix is called when production pathPatternPrefix is exited.
func (s *BaseGQLListener) ExitPathPatternPrefix(ctx *PathPatternPrefixContext) {}

// EnterPathModePrefix is called when production pathModePrefix is entered.
func (s *BaseGQLListener) EnterPathModePrefix(ctx *PathModePrefixContext) {}

// ExitPathModePrefix is called when production pathModePrefix is exited.
func (s *BaseGQLListener) ExitPathModePrefix(ctx *PathModePrefixContext) {}

// EnterPathMode is called when production pathMode is entered.
func (s *BaseGQLListener) EnterPathMode(ctx *PathModeContext) {}

// ExitPathMode is called when production pathMode is exited.
func (s *BaseGQLListener) ExitPathMode(ctx *PathModeContext) {}

// EnterPathSearchPrefix is called when production pathSearchPrefix is entered.
func (s *BaseGQLListener) EnterPathSearchPrefix(ctx *PathSearchPrefixContext) {}

// ExitPathSearchPrefix is called when production pathSearchPrefix is exited.
func (s *BaseGQLListener) ExitPathSearchPrefix(ctx *PathSearchPrefixContext) {}

// EnterAllPathSearch is called when production allPathSearch is entered.
func (s *BaseGQLListener) EnterAllPathSearch(ctx *AllPathSearchContext) {}

// ExitAllPathSearch is called when production allPathSearch is exited.
func (s *BaseGQLListener) ExitAllPathSearch(ctx *AllPathSearchContext) {}

// EnterPathOrPaths is called when production pathOrPaths is entered.
func (s *BaseGQLListener) EnterPathOrPaths(ctx *PathOrPathsContext) {}

// ExitPathOrPaths is called when production pathOrPaths is exited.
func (s *BaseGQLListener) ExitPathOrPaths(ctx *PathOrPathsContext) {}

// EnterAnyPathSearch is called when production anyPathSearch is entered.
func (s *BaseGQLListener) EnterAnyPathSearch(ctx *AnyPathSearchContext) {}

// ExitAnyPathSearch is called when production anyPathSearch is exited.
func (s *BaseGQLListener) ExitAnyPathSearch(ctx *AnyPathSearchContext) {}

// EnterNumberOfPaths is called when production numberOfPaths is entered.
func (s *BaseGQLListener) EnterNumberOfPaths(ctx *NumberOfPathsContext) {}

// ExitNumberOfPaths is called when production numberOfPaths is exited.
func (s *BaseGQLListener) ExitNumberOfPaths(ctx *NumberOfPathsContext) {}

// EnterShortestPathSearch is called when production shortestPathSearch is entered.
func (s *BaseGQLListener) EnterShortestPathSearch(ctx *ShortestPathSearchContext) {}

// ExitShortestPathSearch is called when production shortestPathSearch is exited.
func (s *BaseGQLListener) ExitShortestPathSearch(ctx *ShortestPathSearchContext) {}

// EnterAllShortestPathSearch is called when production allShortestPathSearch is entered.
func (s *BaseGQLListener) EnterAllShortestPathSearch(ctx *AllShortestPathSearchContext) {}

// ExitAllShortestPathSearch is called when production allShortestPathSearch is exited.
func (s *BaseGQLListener) ExitAllShortestPathSearch(ctx *AllShortestPathSearchContext) {}

// EnterAnyShortestPathSearch is called when production anyShortestPathSearch is entered.
func (s *BaseGQLListener) EnterAnyShortestPathSearch(ctx *AnyShortestPathSearchContext) {}

// ExitAnyShortestPathSearch is called when production anyShortestPathSearch is exited.
func (s *BaseGQLListener) ExitAnyShortestPathSearch(ctx *AnyShortestPathSearchContext) {}

// EnterCountedShortestPathSearch is called when production countedShortestPathSearch is entered.
func (s *BaseGQLListener) EnterCountedShortestPathSearch(ctx *CountedShortestPathSearchContext) {}

// ExitCountedShortestPathSearch is called when production countedShortestPathSearch is exited.
func (s *BaseGQLListener) ExitCountedShortestPathSearch(ctx *CountedShortestPathSearchContext) {}

// EnterCountedShortestGroupSearch is called when production countedShortestGroupSearch is entered.
func (s *BaseGQLListener) EnterCountedShortestGroupSearch(ctx *CountedShortestGroupSearchContext) {}

// ExitCountedShortestGroupSearch is called when production countedShortestGroupSearch is exited.
func (s *BaseGQLListener) ExitCountedShortestGroupSearch(ctx *CountedShortestGroupSearchContext) {}

// EnterNumberOfGroups is called when production numberOfGroups is entered.
func (s *BaseGQLListener) EnterNumberOfGroups(ctx *NumberOfGroupsContext) {}

// ExitNumberOfGroups is called when production numberOfGroups is exited.
func (s *BaseGQLListener) ExitNumberOfGroups(ctx *NumberOfGroupsContext) {}

// EnterPpePathTerm is called when production ppePathTerm is entered.
func (s *BaseGQLListener) EnterPpePathTerm(ctx *PpePathTermContext) {}

// ExitPpePathTerm is called when production ppePathTerm is exited.
func (s *BaseGQLListener) ExitPpePathTerm(ctx *PpePathTermContext) {}

// EnterPpeMultisetAlternation is called when production ppeMultisetAlternation is entered.
func (s *BaseGQLListener) EnterPpeMultisetAlternation(ctx *PpeMultisetAlternationContext) {}

// ExitPpeMultisetAlternation is called when production ppeMultisetAlternation is exited.
func (s *BaseGQLListener) ExitPpeMultisetAlternation(ctx *PpeMultisetAlternationContext) {}

// EnterPpePatternUnion is called when production ppePatternUnion is entered.
func (s *BaseGQLListener) EnterPpePatternUnion(ctx *PpePatternUnionContext) {}

// ExitPpePatternUnion is called when production ppePatternUnion is exited.
func (s *BaseGQLListener) ExitPpePatternUnion(ctx *PpePatternUnionContext) {}

// EnterPathTerm is called when production pathTerm is entered.
func (s *BaseGQLListener) EnterPathTerm(ctx *PathTermContext) {}

// ExitPathTerm is called when production pathTerm is exited.
func (s *BaseGQLListener) ExitPathTerm(ctx *PathTermContext) {}

// EnterPfPathPrimary is called when production pfPathPrimary is entered.
func (s *BaseGQLListener) EnterPfPathPrimary(ctx *PfPathPrimaryContext) {}

// ExitPfPathPrimary is called when production pfPathPrimary is exited.
func (s *BaseGQLListener) ExitPfPathPrimary(ctx *PfPathPrimaryContext) {}

// EnterPfQuantifiedPathPrimary is called when production pfQuantifiedPathPrimary is entered.
func (s *BaseGQLListener) EnterPfQuantifiedPathPrimary(ctx *PfQuantifiedPathPrimaryContext) {}

// ExitPfQuantifiedPathPrimary is called when production pfQuantifiedPathPrimary is exited.
func (s *BaseGQLListener) ExitPfQuantifiedPathPrimary(ctx *PfQuantifiedPathPrimaryContext) {}

// EnterPfQuestionedPathPrimary is called when production pfQuestionedPathPrimary is entered.
func (s *BaseGQLListener) EnterPfQuestionedPathPrimary(ctx *PfQuestionedPathPrimaryContext) {}

// ExitPfQuestionedPathPrimary is called when production pfQuestionedPathPrimary is exited.
func (s *BaseGQLListener) ExitPfQuestionedPathPrimary(ctx *PfQuestionedPathPrimaryContext) {}

// EnterPpElementPattern is called when production ppElementPattern is entered.
func (s *BaseGQLListener) EnterPpElementPattern(ctx *PpElementPatternContext) {}

// ExitPpElementPattern is called when production ppElementPattern is exited.
func (s *BaseGQLListener) ExitPpElementPattern(ctx *PpElementPatternContext) {}

// EnterPpParenthesizedPathPatternExpression is called when production ppParenthesizedPathPatternExpression is entered.
func (s *BaseGQLListener) EnterPpParenthesizedPathPatternExpression(ctx *PpParenthesizedPathPatternExpressionContext) {
}

// ExitPpParenthesizedPathPatternExpression is called when production ppParenthesizedPathPatternExpression is exited.
func (s *BaseGQLListener) ExitPpParenthesizedPathPatternExpression(ctx *PpParenthesizedPathPatternExpressionContext) {
}

// EnterPpSimplifiedPathPatternExpression is called when production ppSimplifiedPathPatternExpression is entered.
func (s *BaseGQLListener) EnterPpSimplifiedPathPatternExpression(ctx *PpSimplifiedPathPatternExpressionContext) {
}

// ExitPpSimplifiedPathPatternExpression is called when production ppSimplifiedPathPatternExpression is exited.
func (s *BaseGQLListener) ExitPpSimplifiedPathPatternExpression(ctx *PpSimplifiedPathPatternExpressionContext) {
}

// EnterElementPattern is called when production elementPattern is entered.
func (s *BaseGQLListener) EnterElementPattern(ctx *ElementPatternContext) {}

// ExitElementPattern is called when production elementPattern is exited.
func (s *BaseGQLListener) ExitElementPattern(ctx *ElementPatternContext) {}

// EnterNodePattern is called when production nodePattern is entered.
func (s *BaseGQLListener) EnterNodePattern(ctx *NodePatternContext) {}

// ExitNodePattern is called when production nodePattern is exited.
func (s *BaseGQLListener) ExitNodePattern(ctx *NodePatternContext) {}

// EnterElementPatternFiller is called when production elementPatternFiller is entered.
func (s *BaseGQLListener) EnterElementPatternFiller(ctx *ElementPatternFillerContext) {}

// ExitElementPatternFiller is called when production elementPatternFiller is exited.
func (s *BaseGQLListener) ExitElementPatternFiller(ctx *ElementPatternFillerContext) {}

// EnterElementVariableDeclaration is called when production elementVariableDeclaration is entered.
func (s *BaseGQLListener) EnterElementVariableDeclaration(ctx *ElementVariableDeclarationContext) {}

// ExitElementVariableDeclaration is called when production elementVariableDeclaration is exited.
func (s *BaseGQLListener) ExitElementVariableDeclaration(ctx *ElementVariableDeclarationContext) {}

// EnterIsLabelExpression is called when production isLabelExpression is entered.
func (s *BaseGQLListener) EnterIsLabelExpression(ctx *IsLabelExpressionContext) {}

// ExitIsLabelExpression is called when production isLabelExpression is exited.
func (s *BaseGQLListener) ExitIsLabelExpression(ctx *IsLabelExpressionContext) {}

// EnterIsOrColon is called when production isOrColon is entered.
func (s *BaseGQLListener) EnterIsOrColon(ctx *IsOrColonContext) {}

// ExitIsOrColon is called when production isOrColon is exited.
func (s *BaseGQLListener) ExitIsOrColon(ctx *IsOrColonContext) {}

// EnterElementPatternPredicate is called when production elementPatternPredicate is entered.
func (s *BaseGQLListener) EnterElementPatternPredicate(ctx *ElementPatternPredicateContext) {}

// ExitElementPatternPredicate is called when production elementPatternPredicate is exited.
func (s *BaseGQLListener) ExitElementPatternPredicate(ctx *ElementPatternPredicateContext) {}

// EnterElementPatternWhereClause is called when production elementPatternWhereClause is entered.
func (s *BaseGQLListener) EnterElementPatternWhereClause(ctx *ElementPatternWhereClauseContext) {}

// ExitElementPatternWhereClause is called when production elementPatternWhereClause is exited.
func (s *BaseGQLListener) ExitElementPatternWhereClause(ctx *ElementPatternWhereClauseContext) {}

// EnterElementPropertySpecification is called when production elementPropertySpecification is entered.
func (s *BaseGQLListener) EnterElementPropertySpecification(ctx *ElementPropertySpecificationContext) {
}

// ExitElementPropertySpecification is called when production elementPropertySpecification is exited.
func (s *BaseGQLListener) ExitElementPropertySpecification(ctx *ElementPropertySpecificationContext) {
}

// EnterPropertyKeyValuePairList is called when production propertyKeyValuePairList is entered.
func (s *BaseGQLListener) EnterPropertyKeyValuePairList(ctx *PropertyKeyValuePairListContext) {}

// ExitPropertyKeyValuePairList is called when production propertyKeyValuePairList is exited.
func (s *BaseGQLListener) ExitPropertyKeyValuePairList(ctx *PropertyKeyValuePairListContext) {}

// EnterPropertyKeyValuePair is called when production propertyKeyValuePair is entered.
func (s *BaseGQLListener) EnterPropertyKeyValuePair(ctx *PropertyKeyValuePairContext) {}

// ExitPropertyKeyValuePair is called when production propertyKeyValuePair is exited.
func (s *BaseGQLListener) ExitPropertyKeyValuePair(ctx *PropertyKeyValuePairContext) {}

// EnterEdgePattern is called when production edgePattern is entered.
func (s *BaseGQLListener) EnterEdgePattern(ctx *EdgePatternContext) {}

// ExitEdgePattern is called when production edgePattern is exited.
func (s *BaseGQLListener) ExitEdgePattern(ctx *EdgePatternContext) {}

// EnterFullEdgePattern is called when production fullEdgePattern is entered.
func (s *BaseGQLListener) EnterFullEdgePattern(ctx *FullEdgePatternContext) {}

// ExitFullEdgePattern is called when production fullEdgePattern is exited.
func (s *BaseGQLListener) ExitFullEdgePattern(ctx *FullEdgePatternContext) {}

// EnterFullEdgePointingLeft is called when production fullEdgePointingLeft is entered.
func (s *BaseGQLListener) EnterFullEdgePointingLeft(ctx *FullEdgePointingLeftContext) {}

// ExitFullEdgePointingLeft is called when production fullEdgePointingLeft is exited.
func (s *BaseGQLListener) ExitFullEdgePointingLeft(ctx *FullEdgePointingLeftContext) {}

// EnterFullEdgeUndirected is called when production fullEdgeUndirected is entered.
func (s *BaseGQLListener) EnterFullEdgeUndirected(ctx *FullEdgeUndirectedContext) {}

// ExitFullEdgeUndirected is called when production fullEdgeUndirected is exited.
func (s *BaseGQLListener) ExitFullEdgeUndirected(ctx *FullEdgeUndirectedContext) {}

// EnterFullEdgePointingRight is called when production fullEdgePointingRight is entered.
func (s *BaseGQLListener) EnterFullEdgePointingRight(ctx *FullEdgePointingRightContext) {}

// ExitFullEdgePointingRight is called when production fullEdgePointingRight is exited.
func (s *BaseGQLListener) ExitFullEdgePointingRight(ctx *FullEdgePointingRightContext) {}

// EnterFullEdgeLeftOrUndirected is called when production fullEdgeLeftOrUndirected is entered.
func (s *BaseGQLListener) EnterFullEdgeLeftOrUndirected(ctx *FullEdgeLeftOrUndirectedContext) {}

// ExitFullEdgeLeftOrUndirected is called when production fullEdgeLeftOrUndirected is exited.
func (s *BaseGQLListener) ExitFullEdgeLeftOrUndirected(ctx *FullEdgeLeftOrUndirectedContext) {}

// EnterFullEdgeUndirectedOrRight is called when production fullEdgeUndirectedOrRight is entered.
func (s *BaseGQLListener) EnterFullEdgeUndirectedOrRight(ctx *FullEdgeUndirectedOrRightContext) {}

// ExitFullEdgeUndirectedOrRight is called when production fullEdgeUndirectedOrRight is exited.
func (s *BaseGQLListener) ExitFullEdgeUndirectedOrRight(ctx *FullEdgeUndirectedOrRightContext) {}

// EnterFullEdgeLeftOrRight is called when production fullEdgeLeftOrRight is entered.
func (s *BaseGQLListener) EnterFullEdgeLeftOrRight(ctx *FullEdgeLeftOrRightContext) {}

// ExitFullEdgeLeftOrRight is called when production fullEdgeLeftOrRight is exited.
func (s *BaseGQLListener) ExitFullEdgeLeftOrRight(ctx *FullEdgeLeftOrRightContext) {}

// EnterFullEdgeAnyDirection is called when production fullEdgeAnyDirection is entered.
func (s *BaseGQLListener) EnterFullEdgeAnyDirection(ctx *FullEdgeAnyDirectionContext) {}

// ExitFullEdgeAnyDirection is called when production fullEdgeAnyDirection is exited.
func (s *BaseGQLListener) ExitFullEdgeAnyDirection(ctx *FullEdgeAnyDirectionContext) {}

// EnterAbbreviatedEdgePattern is called when production abbreviatedEdgePattern is entered.
func (s *BaseGQLListener) EnterAbbreviatedEdgePattern(ctx *AbbreviatedEdgePatternContext) {}

// ExitAbbreviatedEdgePattern is called when production abbreviatedEdgePattern is exited.
func (s *BaseGQLListener) ExitAbbreviatedEdgePattern(ctx *AbbreviatedEdgePatternContext) {}

// EnterParenthesizedPathPatternExpression is called when production parenthesizedPathPatternExpression is entered.
func (s *BaseGQLListener) EnterParenthesizedPathPatternExpression(ctx *ParenthesizedPathPatternExpressionContext) {
}

// ExitParenthesizedPathPatternExpression is called when production parenthesizedPathPatternExpression is exited.
func (s *BaseGQLListener) ExitParenthesizedPathPatternExpression(ctx *ParenthesizedPathPatternExpressionContext) {
}

// EnterSubpathVariableDeclaration is called when production subpathVariableDeclaration is entered.
func (s *BaseGQLListener) EnterSubpathVariableDeclaration(ctx *SubpathVariableDeclarationContext) {}

// ExitSubpathVariableDeclaration is called when production subpathVariableDeclaration is exited.
func (s *BaseGQLListener) ExitSubpathVariableDeclaration(ctx *SubpathVariableDeclarationContext) {}

// EnterParenthesizedPathPatternWhereClause is called when production parenthesizedPathPatternWhereClause is entered.
func (s *BaseGQLListener) EnterParenthesizedPathPatternWhereClause(ctx *ParenthesizedPathPatternWhereClauseContext) {
}

// ExitParenthesizedPathPatternWhereClause is called when production parenthesizedPathPatternWhereClause is exited.
func (s *BaseGQLListener) ExitParenthesizedPathPatternWhereClause(ctx *ParenthesizedPathPatternWhereClauseContext) {
}

// EnterLabelExpressionNegation is called when production labelExpressionNegation is entered.
func (s *BaseGQLListener) EnterLabelExpressionNegation(ctx *LabelExpressionNegationContext) {}

// ExitLabelExpressionNegation is called when production labelExpressionNegation is exited.
func (s *BaseGQLListener) ExitLabelExpressionNegation(ctx *LabelExpressionNegationContext) {}

// EnterLabelExpressionDisjunction is called when production labelExpressionDisjunction is entered.
func (s *BaseGQLListener) EnterLabelExpressionDisjunction(ctx *LabelExpressionDisjunctionContext) {}

// ExitLabelExpressionDisjunction is called when production labelExpressionDisjunction is exited.
func (s *BaseGQLListener) ExitLabelExpressionDisjunction(ctx *LabelExpressionDisjunctionContext) {}

// EnterLabelExpressionParenthesized is called when production labelExpressionParenthesized is entered.
func (s *BaseGQLListener) EnterLabelExpressionParenthesized(ctx *LabelExpressionParenthesizedContext) {
}

// ExitLabelExpressionParenthesized is called when production labelExpressionParenthesized is exited.
func (s *BaseGQLListener) ExitLabelExpressionParenthesized(ctx *LabelExpressionParenthesizedContext) {
}

// EnterLabelExpressionWildcard is called when production labelExpressionWildcard is entered.
func (s *BaseGQLListener) EnterLabelExpressionWildcard(ctx *LabelExpressionWildcardContext) {}

// ExitLabelExpressionWildcard is called when production labelExpressionWildcard is exited.
func (s *BaseGQLListener) ExitLabelExpressionWildcard(ctx *LabelExpressionWildcardContext) {}

// EnterLabelExpressionConjunction is called when production labelExpressionConjunction is entered.
func (s *BaseGQLListener) EnterLabelExpressionConjunction(ctx *LabelExpressionConjunctionContext) {}

// ExitLabelExpressionConjunction is called when production labelExpressionConjunction is exited.
func (s *BaseGQLListener) ExitLabelExpressionConjunction(ctx *LabelExpressionConjunctionContext) {}

// EnterLabelExpressionName is called when production labelExpressionName is entered.
func (s *BaseGQLListener) EnterLabelExpressionName(ctx *LabelExpressionNameContext) {}

// ExitLabelExpressionName is called when production labelExpressionName is exited.
func (s *BaseGQLListener) ExitLabelExpressionName(ctx *LabelExpressionNameContext) {}

// EnterPathVariableReference is called when production pathVariableReference is entered.
func (s *BaseGQLListener) EnterPathVariableReference(ctx *PathVariableReferenceContext) {}

// ExitPathVariableReference is called when production pathVariableReference is exited.
func (s *BaseGQLListener) ExitPathVariableReference(ctx *PathVariableReferenceContext) {}

// EnterElementVariableReference is called when production elementVariableReference is entered.
func (s *BaseGQLListener) EnterElementVariableReference(ctx *ElementVariableReferenceContext) {}

// ExitElementVariableReference is called when production elementVariableReference is exited.
func (s *BaseGQLListener) ExitElementVariableReference(ctx *ElementVariableReferenceContext) {}

// EnterGraphPatternQuantifier is called when production graphPatternQuantifier is entered.
func (s *BaseGQLListener) EnterGraphPatternQuantifier(ctx *GraphPatternQuantifierContext) {}

// ExitGraphPatternQuantifier is called when production graphPatternQuantifier is exited.
func (s *BaseGQLListener) ExitGraphPatternQuantifier(ctx *GraphPatternQuantifierContext) {}

// EnterFixedQuantifier is called when production fixedQuantifier is entered.
func (s *BaseGQLListener) EnterFixedQuantifier(ctx *FixedQuantifierContext) {}

// ExitFixedQuantifier is called when production fixedQuantifier is exited.
func (s *BaseGQLListener) ExitFixedQuantifier(ctx *FixedQuantifierContext) {}

// EnterGeneralQuantifier is called when production generalQuantifier is entered.
func (s *BaseGQLListener) EnterGeneralQuantifier(ctx *GeneralQuantifierContext) {}

// ExitGeneralQuantifier is called when production generalQuantifier is exited.
func (s *BaseGQLListener) ExitGeneralQuantifier(ctx *GeneralQuantifierContext) {}

// EnterLowerBound is called when production lowerBound is entered.
func (s *BaseGQLListener) EnterLowerBound(ctx *LowerBoundContext) {}

// ExitLowerBound is called when production lowerBound is exited.
func (s *BaseGQLListener) ExitLowerBound(ctx *LowerBoundContext) {}

// EnterUpperBound is called when production upperBound is entered.
func (s *BaseGQLListener) EnterUpperBound(ctx *UpperBoundContext) {}

// ExitUpperBound is called when production upperBound is exited.
func (s *BaseGQLListener) ExitUpperBound(ctx *UpperBoundContext) {}

// EnterSimplifiedPathPatternExpression is called when production simplifiedPathPatternExpression is entered.
func (s *BaseGQLListener) EnterSimplifiedPathPatternExpression(ctx *SimplifiedPathPatternExpressionContext) {
}

// ExitSimplifiedPathPatternExpression is called when production simplifiedPathPatternExpression is exited.
func (s *BaseGQLListener) ExitSimplifiedPathPatternExpression(ctx *SimplifiedPathPatternExpressionContext) {
}

// EnterSimplifiedDefaultingLeft is called when production simplifiedDefaultingLeft is entered.
func (s *BaseGQLListener) EnterSimplifiedDefaultingLeft(ctx *SimplifiedDefaultingLeftContext) {}

// ExitSimplifiedDefaultingLeft is called when production simplifiedDefaultingLeft is exited.
func (s *BaseGQLListener) ExitSimplifiedDefaultingLeft(ctx *SimplifiedDefaultingLeftContext) {}

// EnterSimplifiedDefaultingUndirected is called when production simplifiedDefaultingUndirected is entered.
func (s *BaseGQLListener) EnterSimplifiedDefaultingUndirected(ctx *SimplifiedDefaultingUndirectedContext) {
}

// ExitSimplifiedDefaultingUndirected is called when production simplifiedDefaultingUndirected is exited.
func (s *BaseGQLListener) ExitSimplifiedDefaultingUndirected(ctx *SimplifiedDefaultingUndirectedContext) {
}

// EnterSimplifiedDefaultingRight is called when production simplifiedDefaultingRight is entered.
func (s *BaseGQLListener) EnterSimplifiedDefaultingRight(ctx *SimplifiedDefaultingRightContext) {}

// ExitSimplifiedDefaultingRight is called when production simplifiedDefaultingRight is exited.
func (s *BaseGQLListener) ExitSimplifiedDefaultingRight(ctx *SimplifiedDefaultingRightContext) {}

// EnterSimplifiedDefaultingLeftOrUndirected is called when production simplifiedDefaultingLeftOrUndirected is entered.
func (s *BaseGQLListener) EnterSimplifiedDefaultingLeftOrUndirected(ctx *SimplifiedDefaultingLeftOrUndirectedContext) {
}

// ExitSimplifiedDefaultingLeftOrUndirected is called when production simplifiedDefaultingLeftOrUndirected is exited.
func (s *BaseGQLListener) ExitSimplifiedDefaultingLeftOrUndirected(ctx *SimplifiedDefaultingLeftOrUndirectedContext) {
}

// EnterSimplifiedDefaultingUndirectedOrRight is called when production simplifiedDefaultingUndirectedOrRight is entered.
func (s *BaseGQLListener) EnterSimplifiedDefaultingUndirectedOrRight(ctx *SimplifiedDefaultingUndirectedOrRightContext) {
}

// ExitSimplifiedDefaultingUndirectedOrRight is called when production simplifiedDefaultingUndirectedOrRight is exited.
func (s *BaseGQLListener) ExitSimplifiedDefaultingUndirectedOrRight(ctx *SimplifiedDefaultingUndirectedOrRightContext) {
}

// EnterSimplifiedDefaultingLeftOrRight is called when production simplifiedDefaultingLeftOrRight is entered.
func (s *BaseGQLListener) EnterSimplifiedDefaultingLeftOrRight(ctx *SimplifiedDefaultingLeftOrRightContext) {
}

// ExitSimplifiedDefaultingLeftOrRight is called when production simplifiedDefaultingLeftOrRight is exited.
func (s *BaseGQLListener) ExitSimplifiedDefaultingLeftOrRight(ctx *SimplifiedDefaultingLeftOrRightContext) {
}

// EnterSimplifiedDefaultingAnyDirection is called when production simplifiedDefaultingAnyDirection is entered.
func (s *BaseGQLListener) EnterSimplifiedDefaultingAnyDirection(ctx *SimplifiedDefaultingAnyDirectionContext) {
}

// ExitSimplifiedDefaultingAnyDirection is called when production simplifiedDefaultingAnyDirection is exited.
func (s *BaseGQLListener) ExitSimplifiedDefaultingAnyDirection(ctx *SimplifiedDefaultingAnyDirectionContext) {
}

// EnterSimplifiedContents is called when production simplifiedContents is entered.
func (s *BaseGQLListener) EnterSimplifiedContents(ctx *SimplifiedContentsContext) {}

// ExitSimplifiedContents is called when production simplifiedContents is exited.
func (s *BaseGQLListener) ExitSimplifiedContents(ctx *SimplifiedContentsContext) {}

// EnterSimplifiedPathUnion is called when production simplifiedPathUnion is entered.
func (s *BaseGQLListener) EnterSimplifiedPathUnion(ctx *SimplifiedPathUnionContext) {}

// ExitSimplifiedPathUnion is called when production simplifiedPathUnion is exited.
func (s *BaseGQLListener) ExitSimplifiedPathUnion(ctx *SimplifiedPathUnionContext) {}

// EnterSimplifiedMultisetAlternation is called when production simplifiedMultisetAlternation is entered.
func (s *BaseGQLListener) EnterSimplifiedMultisetAlternation(ctx *SimplifiedMultisetAlternationContext) {
}

// ExitSimplifiedMultisetAlternation is called when production simplifiedMultisetAlternation is exited.
func (s *BaseGQLListener) ExitSimplifiedMultisetAlternation(ctx *SimplifiedMultisetAlternationContext) {
}

// EnterSimplifiedFactorLowLabel is called when production simplifiedFactorLowLabel is entered.
func (s *BaseGQLListener) EnterSimplifiedFactorLowLabel(ctx *SimplifiedFactorLowLabelContext) {}

// ExitSimplifiedFactorLowLabel is called when production simplifiedFactorLowLabel is exited.
func (s *BaseGQLListener) ExitSimplifiedFactorLowLabel(ctx *SimplifiedFactorLowLabelContext) {}

// EnterSimplifiedConcatenationLabel is called when production simplifiedConcatenationLabel is entered.
func (s *BaseGQLListener) EnterSimplifiedConcatenationLabel(ctx *SimplifiedConcatenationLabelContext) {
}

// ExitSimplifiedConcatenationLabel is called when production simplifiedConcatenationLabel is exited.
func (s *BaseGQLListener) ExitSimplifiedConcatenationLabel(ctx *SimplifiedConcatenationLabelContext) {
}

// EnterSimplifiedConjunctionLabel is called when production simplifiedConjunctionLabel is entered.
func (s *BaseGQLListener) EnterSimplifiedConjunctionLabel(ctx *SimplifiedConjunctionLabelContext) {}

// ExitSimplifiedConjunctionLabel is called when production simplifiedConjunctionLabel is exited.
func (s *BaseGQLListener) ExitSimplifiedConjunctionLabel(ctx *SimplifiedConjunctionLabelContext) {}

// EnterSimplifiedFactorHighLabel is called when production simplifiedFactorHighLabel is entered.
func (s *BaseGQLListener) EnterSimplifiedFactorHighLabel(ctx *SimplifiedFactorHighLabelContext) {}

// ExitSimplifiedFactorHighLabel is called when production simplifiedFactorHighLabel is exited.
func (s *BaseGQLListener) ExitSimplifiedFactorHighLabel(ctx *SimplifiedFactorHighLabelContext) {}

// EnterSimplifiedFactorHigh is called when production simplifiedFactorHigh is entered.
func (s *BaseGQLListener) EnterSimplifiedFactorHigh(ctx *SimplifiedFactorHighContext) {}

// ExitSimplifiedFactorHigh is called when production simplifiedFactorHigh is exited.
func (s *BaseGQLListener) ExitSimplifiedFactorHigh(ctx *SimplifiedFactorHighContext) {}

// EnterSimplifiedQuantified is called when production simplifiedQuantified is entered.
func (s *BaseGQLListener) EnterSimplifiedQuantified(ctx *SimplifiedQuantifiedContext) {}

// ExitSimplifiedQuantified is called when production simplifiedQuantified is exited.
func (s *BaseGQLListener) ExitSimplifiedQuantified(ctx *SimplifiedQuantifiedContext) {}

// EnterSimplifiedQuestioned is called when production simplifiedQuestioned is entered.
func (s *BaseGQLListener) EnterSimplifiedQuestioned(ctx *SimplifiedQuestionedContext) {}

// ExitSimplifiedQuestioned is called when production simplifiedQuestioned is exited.
func (s *BaseGQLListener) ExitSimplifiedQuestioned(ctx *SimplifiedQuestionedContext) {}

// EnterSimplifiedTertiary is called when production simplifiedTertiary is entered.
func (s *BaseGQLListener) EnterSimplifiedTertiary(ctx *SimplifiedTertiaryContext) {}

// ExitSimplifiedTertiary is called when production simplifiedTertiary is exited.
func (s *BaseGQLListener) ExitSimplifiedTertiary(ctx *SimplifiedTertiaryContext) {}

// EnterSimplifiedDirectionOverride is called when production simplifiedDirectionOverride is entered.
func (s *BaseGQLListener) EnterSimplifiedDirectionOverride(ctx *SimplifiedDirectionOverrideContext) {}

// ExitSimplifiedDirectionOverride is called when production simplifiedDirectionOverride is exited.
func (s *BaseGQLListener) ExitSimplifiedDirectionOverride(ctx *SimplifiedDirectionOverrideContext) {}

// EnterSimplifiedOverrideLeft is called when production simplifiedOverrideLeft is entered.
func (s *BaseGQLListener) EnterSimplifiedOverrideLeft(ctx *SimplifiedOverrideLeftContext) {}

// ExitSimplifiedOverrideLeft is called when production simplifiedOverrideLeft is exited.
func (s *BaseGQLListener) ExitSimplifiedOverrideLeft(ctx *SimplifiedOverrideLeftContext) {}

// EnterSimplifiedOverrideUndirected is called when production simplifiedOverrideUndirected is entered.
func (s *BaseGQLListener) EnterSimplifiedOverrideUndirected(ctx *SimplifiedOverrideUndirectedContext) {
}

// ExitSimplifiedOverrideUndirected is called when production simplifiedOverrideUndirected is exited.
func (s *BaseGQLListener) ExitSimplifiedOverrideUndirected(ctx *SimplifiedOverrideUndirectedContext) {
}

// EnterSimplifiedOverrideRight is called when production simplifiedOverrideRight is entered.
func (s *BaseGQLListener) EnterSimplifiedOverrideRight(ctx *SimplifiedOverrideRightContext) {}

// ExitSimplifiedOverrideRight is called when production simplifiedOverrideRight is exited.
func (s *BaseGQLListener) ExitSimplifiedOverrideRight(ctx *SimplifiedOverrideRightContext) {}

// EnterSimplifiedOverrideLeftOrUndirected is called when production simplifiedOverrideLeftOrUndirected is entered.
func (s *BaseGQLListener) EnterSimplifiedOverrideLeftOrUndirected(ctx *SimplifiedOverrideLeftOrUndirectedContext) {
}

// ExitSimplifiedOverrideLeftOrUndirected is called when production simplifiedOverrideLeftOrUndirected is exited.
func (s *BaseGQLListener) ExitSimplifiedOverrideLeftOrUndirected(ctx *SimplifiedOverrideLeftOrUndirectedContext) {
}

// EnterSimplifiedOverrideUndirectedOrRight is called when production simplifiedOverrideUndirectedOrRight is entered.
func (s *BaseGQLListener) EnterSimplifiedOverrideUndirectedOrRight(ctx *SimplifiedOverrideUndirectedOrRightContext) {
}

// ExitSimplifiedOverrideUndirectedOrRight is called when production simplifiedOverrideUndirectedOrRight is exited.
func (s *BaseGQLListener) ExitSimplifiedOverrideUndirectedOrRight(ctx *SimplifiedOverrideUndirectedOrRightContext) {
}

// EnterSimplifiedOverrideLeftOrRight is called when production simplifiedOverrideLeftOrRight is entered.
func (s *BaseGQLListener) EnterSimplifiedOverrideLeftOrRight(ctx *SimplifiedOverrideLeftOrRightContext) {
}

// ExitSimplifiedOverrideLeftOrRight is called when production simplifiedOverrideLeftOrRight is exited.
func (s *BaseGQLListener) ExitSimplifiedOverrideLeftOrRight(ctx *SimplifiedOverrideLeftOrRightContext) {
}

// EnterSimplifiedOverrideAnyDirection is called when production simplifiedOverrideAnyDirection is entered.
func (s *BaseGQLListener) EnterSimplifiedOverrideAnyDirection(ctx *SimplifiedOverrideAnyDirectionContext) {
}

// ExitSimplifiedOverrideAnyDirection is called when production simplifiedOverrideAnyDirection is exited.
func (s *BaseGQLListener) ExitSimplifiedOverrideAnyDirection(ctx *SimplifiedOverrideAnyDirectionContext) {
}

// EnterSimplifiedSecondary is called when production simplifiedSecondary is entered.
func (s *BaseGQLListener) EnterSimplifiedSecondary(ctx *SimplifiedSecondaryContext) {}

// ExitSimplifiedSecondary is called when production simplifiedSecondary is exited.
func (s *BaseGQLListener) ExitSimplifiedSecondary(ctx *SimplifiedSecondaryContext) {}

// EnterSimplifiedNegation is called when production simplifiedNegation is entered.
func (s *BaseGQLListener) EnterSimplifiedNegation(ctx *SimplifiedNegationContext) {}

// ExitSimplifiedNegation is called when production simplifiedNegation is exited.
func (s *BaseGQLListener) ExitSimplifiedNegation(ctx *SimplifiedNegationContext) {}

// EnterSimplifiedPrimary is called when production simplifiedPrimary is entered.
func (s *BaseGQLListener) EnterSimplifiedPrimary(ctx *SimplifiedPrimaryContext) {}

// ExitSimplifiedPrimary is called when production simplifiedPrimary is exited.
func (s *BaseGQLListener) ExitSimplifiedPrimary(ctx *SimplifiedPrimaryContext) {}

// EnterWhereClause is called when production whereClause is entered.
func (s *BaseGQLListener) EnterWhereClause(ctx *WhereClauseContext) {}

// ExitWhereClause is called when production whereClause is exited.
func (s *BaseGQLListener) ExitWhereClause(ctx *WhereClauseContext) {}

// EnterYieldClause is called when production yieldClause is entered.
func (s *BaseGQLListener) EnterYieldClause(ctx *YieldClauseContext) {}

// ExitYieldClause is called when production yieldClause is exited.
func (s *BaseGQLListener) ExitYieldClause(ctx *YieldClauseContext) {}

// EnterYieldItemList is called when production yieldItemList is entered.
func (s *BaseGQLListener) EnterYieldItemList(ctx *YieldItemListContext) {}

// ExitYieldItemList is called when production yieldItemList is exited.
func (s *BaseGQLListener) ExitYieldItemList(ctx *YieldItemListContext) {}

// EnterYieldItem is called when production yieldItem is entered.
func (s *BaseGQLListener) EnterYieldItem(ctx *YieldItemContext) {}

// ExitYieldItem is called when production yieldItem is exited.
func (s *BaseGQLListener) ExitYieldItem(ctx *YieldItemContext) {}

// EnterYieldItemName is called when production yieldItemName is entered.
func (s *BaseGQLListener) EnterYieldItemName(ctx *YieldItemNameContext) {}

// ExitYieldItemName is called when production yieldItemName is exited.
func (s *BaseGQLListener) ExitYieldItemName(ctx *YieldItemNameContext) {}

// EnterYieldItemAlias is called when production yieldItemAlias is entered.
func (s *BaseGQLListener) EnterYieldItemAlias(ctx *YieldItemAliasContext) {}

// ExitYieldItemAlias is called when production yieldItemAlias is exited.
func (s *BaseGQLListener) ExitYieldItemAlias(ctx *YieldItemAliasContext) {}

// EnterGroupByClause is called when production groupByClause is entered.
func (s *BaseGQLListener) EnterGroupByClause(ctx *GroupByClauseContext) {}

// ExitGroupByClause is called when production groupByClause is exited.
func (s *BaseGQLListener) ExitGroupByClause(ctx *GroupByClauseContext) {}

// EnterGroupingElementList is called when production groupingElementList is entered.
func (s *BaseGQLListener) EnterGroupingElementList(ctx *GroupingElementListContext) {}

// ExitGroupingElementList is called when production groupingElementList is exited.
func (s *BaseGQLListener) ExitGroupingElementList(ctx *GroupingElementListContext) {}

// EnterGroupingElement is called when production groupingElement is entered.
func (s *BaseGQLListener) EnterGroupingElement(ctx *GroupingElementContext) {}

// ExitGroupingElement is called when production groupingElement is exited.
func (s *BaseGQLListener) ExitGroupingElement(ctx *GroupingElementContext) {}

// EnterEmptyGroupingSet is called when production emptyGroupingSet is entered.
func (s *BaseGQLListener) EnterEmptyGroupingSet(ctx *EmptyGroupingSetContext) {}

// ExitEmptyGroupingSet is called when production emptyGroupingSet is exited.
func (s *BaseGQLListener) ExitEmptyGroupingSet(ctx *EmptyGroupingSetContext) {}

// EnterOrderByClause is called when production orderByClause is entered.
func (s *BaseGQLListener) EnterOrderByClause(ctx *OrderByClauseContext) {}

// ExitOrderByClause is called when production orderByClause is exited.
func (s *BaseGQLListener) ExitOrderByClause(ctx *OrderByClauseContext) {}

// EnterSortSpecificationList is called when production sortSpecificationList is entered.
func (s *BaseGQLListener) EnterSortSpecificationList(ctx *SortSpecificationListContext) {}

// ExitSortSpecificationList is called when production sortSpecificationList is exited.
func (s *BaseGQLListener) ExitSortSpecificationList(ctx *SortSpecificationListContext) {}

// EnterSortSpecification is called when production sortSpecification is entered.
func (s *BaseGQLListener) EnterSortSpecification(ctx *SortSpecificationContext) {}

// ExitSortSpecification is called when production sortSpecification is exited.
func (s *BaseGQLListener) ExitSortSpecification(ctx *SortSpecificationContext) {}

// EnterSortKey is called when production sortKey is entered.
func (s *BaseGQLListener) EnterSortKey(ctx *SortKeyContext) {}

// ExitSortKey is called when production sortKey is exited.
func (s *BaseGQLListener) ExitSortKey(ctx *SortKeyContext) {}

// EnterOrderingSpecification is called when production orderingSpecification is entered.
func (s *BaseGQLListener) EnterOrderingSpecification(ctx *OrderingSpecificationContext) {}

// ExitOrderingSpecification is called when production orderingSpecification is exited.
func (s *BaseGQLListener) ExitOrderingSpecification(ctx *OrderingSpecificationContext) {}

// EnterNullOrdering is called when production nullOrdering is entered.
func (s *BaseGQLListener) EnterNullOrdering(ctx *NullOrderingContext) {}

// ExitNullOrdering is called when production nullOrdering is exited.
func (s *BaseGQLListener) ExitNullOrdering(ctx *NullOrderingContext) {}

// EnterLimitClause is called when production limitClause is entered.
func (s *BaseGQLListener) EnterLimitClause(ctx *LimitClauseContext) {}

// ExitLimitClause is called when production limitClause is exited.
func (s *BaseGQLListener) ExitLimitClause(ctx *LimitClauseContext) {}

// EnterOffsetClause is called when production offsetClause is entered.
func (s *BaseGQLListener) EnterOffsetClause(ctx *OffsetClauseContext) {}

// ExitOffsetClause is called when production offsetClause is exited.
func (s *BaseGQLListener) ExitOffsetClause(ctx *OffsetClauseContext) {}

// EnterOffsetSynonym is called when production offsetSynonym is entered.
func (s *BaseGQLListener) EnterOffsetSynonym(ctx *OffsetSynonymContext) {}

// ExitOffsetSynonym is called when production offsetSynonym is exited.
func (s *BaseGQLListener) ExitOffsetSynonym(ctx *OffsetSynonymContext) {}

// EnterSchemaReference is called when production schemaReference is entered.
func (s *BaseGQLListener) EnterSchemaReference(ctx *SchemaReferenceContext) {}

// ExitSchemaReference is called when production schemaReference is exited.
func (s *BaseGQLListener) ExitSchemaReference(ctx *SchemaReferenceContext) {}

// EnterAbsoluteCatalogSchemaReference is called when production absoluteCatalogSchemaReference is entered.
func (s *BaseGQLListener) EnterAbsoluteCatalogSchemaReference(ctx *AbsoluteCatalogSchemaReferenceContext) {
}

// ExitAbsoluteCatalogSchemaReference is called when production absoluteCatalogSchemaReference is exited.
func (s *BaseGQLListener) ExitAbsoluteCatalogSchemaReference(ctx *AbsoluteCatalogSchemaReferenceContext) {
}

// EnterCatalogSchemaParentAndName is called when production catalogSchemaParentAndName is entered.
func (s *BaseGQLListener) EnterCatalogSchemaParentAndName(ctx *CatalogSchemaParentAndNameContext) {}

// ExitCatalogSchemaParentAndName is called when production catalogSchemaParentAndName is exited.
func (s *BaseGQLListener) ExitCatalogSchemaParentAndName(ctx *CatalogSchemaParentAndNameContext) {}

// EnterRelativeCatalogSchemaReference is called when production relativeCatalogSchemaReference is entered.
func (s *BaseGQLListener) EnterRelativeCatalogSchemaReference(ctx *RelativeCatalogSchemaReferenceContext) {
}

// ExitRelativeCatalogSchemaReference is called when production relativeCatalogSchemaReference is exited.
func (s *BaseGQLListener) ExitRelativeCatalogSchemaReference(ctx *RelativeCatalogSchemaReferenceContext) {
}

// EnterPredefinedSchemaReference is called when production predefinedSchemaReference is entered.
func (s *BaseGQLListener) EnterPredefinedSchemaReference(ctx *PredefinedSchemaReferenceContext) {}

// ExitPredefinedSchemaReference is called when production predefinedSchemaReference is exited.
func (s *BaseGQLListener) ExitPredefinedSchemaReference(ctx *PredefinedSchemaReferenceContext) {}

// EnterAbsoluteDirectoryPath is called when production absoluteDirectoryPath is entered.
func (s *BaseGQLListener) EnterAbsoluteDirectoryPath(ctx *AbsoluteDirectoryPathContext) {}

// ExitAbsoluteDirectoryPath is called when production absoluteDirectoryPath is exited.
func (s *BaseGQLListener) ExitAbsoluteDirectoryPath(ctx *AbsoluteDirectoryPathContext) {}

// EnterRelativeDirectoryPath is called when production relativeDirectoryPath is entered.
func (s *BaseGQLListener) EnterRelativeDirectoryPath(ctx *RelativeDirectoryPathContext) {}

// ExitRelativeDirectoryPath is called when production relativeDirectoryPath is exited.
func (s *BaseGQLListener) ExitRelativeDirectoryPath(ctx *RelativeDirectoryPathContext) {}

// EnterSimpleDirectoryPath is called when production simpleDirectoryPath is entered.
func (s *BaseGQLListener) EnterSimpleDirectoryPath(ctx *SimpleDirectoryPathContext) {}

// ExitSimpleDirectoryPath is called when production simpleDirectoryPath is exited.
func (s *BaseGQLListener) ExitSimpleDirectoryPath(ctx *SimpleDirectoryPathContext) {}

// EnterGraphReference is called when production graphReference is entered.
func (s *BaseGQLListener) EnterGraphReference(ctx *GraphReferenceContext) {}

// ExitGraphReference is called when production graphReference is exited.
func (s *BaseGQLListener) ExitGraphReference(ctx *GraphReferenceContext) {}

// EnterCatalogGraphParentAndName is called when production catalogGraphParentAndName is entered.
func (s *BaseGQLListener) EnterCatalogGraphParentAndName(ctx *CatalogGraphParentAndNameContext) {}

// ExitCatalogGraphParentAndName is called when production catalogGraphParentAndName is exited.
func (s *BaseGQLListener) ExitCatalogGraphParentAndName(ctx *CatalogGraphParentAndNameContext) {}

// EnterHomeGraph is called when production homeGraph is entered.
func (s *BaseGQLListener) EnterHomeGraph(ctx *HomeGraphContext) {}

// ExitHomeGraph is called when production homeGraph is exited.
func (s *BaseGQLListener) ExitHomeGraph(ctx *HomeGraphContext) {}

// EnterGraphTypeReference is called when production graphTypeReference is entered.
func (s *BaseGQLListener) EnterGraphTypeReference(ctx *GraphTypeReferenceContext) {}

// ExitGraphTypeReference is called when production graphTypeReference is exited.
func (s *BaseGQLListener) ExitGraphTypeReference(ctx *GraphTypeReferenceContext) {}

// EnterCatalogGraphTypeParentAndName is called when production catalogGraphTypeParentAndName is entered.
func (s *BaseGQLListener) EnterCatalogGraphTypeParentAndName(ctx *CatalogGraphTypeParentAndNameContext) {
}

// ExitCatalogGraphTypeParentAndName is called when production catalogGraphTypeParentAndName is exited.
func (s *BaseGQLListener) ExitCatalogGraphTypeParentAndName(ctx *CatalogGraphTypeParentAndNameContext) {
}

// EnterBindingTableReference is called when production bindingTableReference is entered.
func (s *BaseGQLListener) EnterBindingTableReference(ctx *BindingTableReferenceContext) {}

// ExitBindingTableReference is called when production bindingTableReference is exited.
func (s *BaseGQLListener) ExitBindingTableReference(ctx *BindingTableReferenceContext) {}

// EnterProcedureReference is called when production procedureReference is entered.
func (s *BaseGQLListener) EnterProcedureReference(ctx *ProcedureReferenceContext) {}

// ExitProcedureReference is called when production procedureReference is exited.
func (s *BaseGQLListener) ExitProcedureReference(ctx *ProcedureReferenceContext) {}

// EnterCatalogProcedureParentAndName is called when production catalogProcedureParentAndName is entered.
func (s *BaseGQLListener) EnterCatalogProcedureParentAndName(ctx *CatalogProcedureParentAndNameContext) {
}

// ExitCatalogProcedureParentAndName is called when production catalogProcedureParentAndName is exited.
func (s *BaseGQLListener) ExitCatalogProcedureParentAndName(ctx *CatalogProcedureParentAndNameContext) {
}

// EnterCatalogObjectParentReference is called when production catalogObjectParentReference is entered.
func (s *BaseGQLListener) EnterCatalogObjectParentReference(ctx *CatalogObjectParentReferenceContext) {
}

// ExitCatalogObjectParentReference is called when production catalogObjectParentReference is exited.
func (s *BaseGQLListener) ExitCatalogObjectParentReference(ctx *CatalogObjectParentReferenceContext) {
}

// EnterReferenceParameterSpecification is called when production referenceParameterSpecification is entered.
func (s *BaseGQLListener) EnterReferenceParameterSpecification(ctx *ReferenceParameterSpecificationContext) {
}

// ExitReferenceParameterSpecification is called when production referenceParameterSpecification is exited.
func (s *BaseGQLListener) ExitReferenceParameterSpecification(ctx *ReferenceParameterSpecificationContext) {
}

// EnterNestedGraphTypeSpecification is called when production nestedGraphTypeSpecification is entered.
func (s *BaseGQLListener) EnterNestedGraphTypeSpecification(ctx *NestedGraphTypeSpecificationContext) {
}

// ExitNestedGraphTypeSpecification is called when production nestedGraphTypeSpecification is exited.
func (s *BaseGQLListener) ExitNestedGraphTypeSpecification(ctx *NestedGraphTypeSpecificationContext) {
}

// EnterGraphTypeSpecificationBody is called when production graphTypeSpecificationBody is entered.
func (s *BaseGQLListener) EnterGraphTypeSpecificationBody(ctx *GraphTypeSpecificationBodyContext) {}

// ExitGraphTypeSpecificationBody is called when production graphTypeSpecificationBody is exited.
func (s *BaseGQLListener) ExitGraphTypeSpecificationBody(ctx *GraphTypeSpecificationBodyContext) {}

// EnterElementTypeList is called when production elementTypeList is entered.
func (s *BaseGQLListener) EnterElementTypeList(ctx *ElementTypeListContext) {}

// ExitElementTypeList is called when production elementTypeList is exited.
func (s *BaseGQLListener) ExitElementTypeList(ctx *ElementTypeListContext) {}

// EnterElementTypeSpecification is called when production elementTypeSpecification is entered.
func (s *BaseGQLListener) EnterElementTypeSpecification(ctx *ElementTypeSpecificationContext) {}

// ExitElementTypeSpecification is called when production elementTypeSpecification is exited.
func (s *BaseGQLListener) ExitElementTypeSpecification(ctx *ElementTypeSpecificationContext) {}

// EnterNodeTypeSpecification is called when production nodeTypeSpecification is entered.
func (s *BaseGQLListener) EnterNodeTypeSpecification(ctx *NodeTypeSpecificationContext) {}

// ExitNodeTypeSpecification is called when production nodeTypeSpecification is exited.
func (s *BaseGQLListener) ExitNodeTypeSpecification(ctx *NodeTypeSpecificationContext) {}

// EnterNodeTypePattern is called when production nodeTypePattern is entered.
func (s *BaseGQLListener) EnterNodeTypePattern(ctx *NodeTypePatternContext) {}

// ExitNodeTypePattern is called when production nodeTypePattern is exited.
func (s *BaseGQLListener) ExitNodeTypePattern(ctx *NodeTypePatternContext) {}

// EnterNodeTypePhrase is called when production nodeTypePhrase is entered.
func (s *BaseGQLListener) EnterNodeTypePhrase(ctx *NodeTypePhraseContext) {}

// ExitNodeTypePhrase is called when production nodeTypePhrase is exited.
func (s *BaseGQLListener) ExitNodeTypePhrase(ctx *NodeTypePhraseContext) {}

// EnterNodeTypePhraseFiller is called when production nodeTypePhraseFiller is entered.
func (s *BaseGQLListener) EnterNodeTypePhraseFiller(ctx *NodeTypePhraseFillerContext) {}

// ExitNodeTypePhraseFiller is called when production nodeTypePhraseFiller is exited.
func (s *BaseGQLListener) ExitNodeTypePhraseFiller(ctx *NodeTypePhraseFillerContext) {}

// EnterNodeTypeFiller is called when production nodeTypeFiller is entered.
func (s *BaseGQLListener) EnterNodeTypeFiller(ctx *NodeTypeFillerContext) {}

// ExitNodeTypeFiller is called when production nodeTypeFiller is exited.
func (s *BaseGQLListener) ExitNodeTypeFiller(ctx *NodeTypeFillerContext) {}

// EnterLocalNodeTypeAlias is called when production localNodeTypeAlias is entered.
func (s *BaseGQLListener) EnterLocalNodeTypeAlias(ctx *LocalNodeTypeAliasContext) {}

// ExitLocalNodeTypeAlias is called when production localNodeTypeAlias is exited.
func (s *BaseGQLListener) ExitLocalNodeTypeAlias(ctx *LocalNodeTypeAliasContext) {}

// EnterNodeTypeImpliedContent is called when production nodeTypeImpliedContent is entered.
func (s *BaseGQLListener) EnterNodeTypeImpliedContent(ctx *NodeTypeImpliedContentContext) {}

// ExitNodeTypeImpliedContent is called when production nodeTypeImpliedContent is exited.
func (s *BaseGQLListener) ExitNodeTypeImpliedContent(ctx *NodeTypeImpliedContentContext) {}

// EnterNodeTypeKeyLabelSet is called when production nodeTypeKeyLabelSet is entered.
func (s *BaseGQLListener) EnterNodeTypeKeyLabelSet(ctx *NodeTypeKeyLabelSetContext) {}

// ExitNodeTypeKeyLabelSet is called when production nodeTypeKeyLabelSet is exited.
func (s *BaseGQLListener) ExitNodeTypeKeyLabelSet(ctx *NodeTypeKeyLabelSetContext) {}

// EnterNodeTypeLabelSet is called when production nodeTypeLabelSet is entered.
func (s *BaseGQLListener) EnterNodeTypeLabelSet(ctx *NodeTypeLabelSetContext) {}

// ExitNodeTypeLabelSet is called when production nodeTypeLabelSet is exited.
func (s *BaseGQLListener) ExitNodeTypeLabelSet(ctx *NodeTypeLabelSetContext) {}

// EnterNodeTypePropertyTypes is called when production nodeTypePropertyTypes is entered.
func (s *BaseGQLListener) EnterNodeTypePropertyTypes(ctx *NodeTypePropertyTypesContext) {}

// ExitNodeTypePropertyTypes is called when production nodeTypePropertyTypes is exited.
func (s *BaseGQLListener) ExitNodeTypePropertyTypes(ctx *NodeTypePropertyTypesContext) {}

// EnterEdgeTypeSpecification is called when production edgeTypeSpecification is entered.
func (s *BaseGQLListener) EnterEdgeTypeSpecification(ctx *EdgeTypeSpecificationContext) {}

// ExitEdgeTypeSpecification is called when production edgeTypeSpecification is exited.
func (s *BaseGQLListener) ExitEdgeTypeSpecification(ctx *EdgeTypeSpecificationContext) {}

// EnterEdgeTypePattern is called when production edgeTypePattern is entered.
func (s *BaseGQLListener) EnterEdgeTypePattern(ctx *EdgeTypePatternContext) {}

// ExitEdgeTypePattern is called when production edgeTypePattern is exited.
func (s *BaseGQLListener) ExitEdgeTypePattern(ctx *EdgeTypePatternContext) {}

// EnterEdgeTypePhrase is called when production edgeTypePhrase is entered.
func (s *BaseGQLListener) EnterEdgeTypePhrase(ctx *EdgeTypePhraseContext) {}

// ExitEdgeTypePhrase is called when production edgeTypePhrase is exited.
func (s *BaseGQLListener) ExitEdgeTypePhrase(ctx *EdgeTypePhraseContext) {}

// EnterEdgeTypePhraseFiller is called when production edgeTypePhraseFiller is entered.
func (s *BaseGQLListener) EnterEdgeTypePhraseFiller(ctx *EdgeTypePhraseFillerContext) {}

// ExitEdgeTypePhraseFiller is called when production edgeTypePhraseFiller is exited.
func (s *BaseGQLListener) ExitEdgeTypePhraseFiller(ctx *EdgeTypePhraseFillerContext) {}

// EnterEdgeTypeFiller is called when production edgeTypeFiller is entered.
func (s *BaseGQLListener) EnterEdgeTypeFiller(ctx *EdgeTypeFillerContext) {}

// ExitEdgeTypeFiller is called when production edgeTypeFiller is exited.
func (s *BaseGQLListener) ExitEdgeTypeFiller(ctx *EdgeTypeFillerContext) {}

// EnterEdgeTypeImpliedContent is called when production edgeTypeImpliedContent is entered.
func (s *BaseGQLListener) EnterEdgeTypeImpliedContent(ctx *EdgeTypeImpliedContentContext) {}

// ExitEdgeTypeImpliedContent is called when production edgeTypeImpliedContent is exited.
func (s *BaseGQLListener) ExitEdgeTypeImpliedContent(ctx *EdgeTypeImpliedContentContext) {}

// EnterEdgeTypeKeyLabelSet is called when production edgeTypeKeyLabelSet is entered.
func (s *BaseGQLListener) EnterEdgeTypeKeyLabelSet(ctx *EdgeTypeKeyLabelSetContext) {}

// ExitEdgeTypeKeyLabelSet is called when production edgeTypeKeyLabelSet is exited.
func (s *BaseGQLListener) ExitEdgeTypeKeyLabelSet(ctx *EdgeTypeKeyLabelSetContext) {}

// EnterEdgeTypeLabelSet is called when production edgeTypeLabelSet is entered.
func (s *BaseGQLListener) EnterEdgeTypeLabelSet(ctx *EdgeTypeLabelSetContext) {}

// ExitEdgeTypeLabelSet is called when production edgeTypeLabelSet is exited.
func (s *BaseGQLListener) ExitEdgeTypeLabelSet(ctx *EdgeTypeLabelSetContext) {}

// EnterEdgeTypePropertyTypes is called when production edgeTypePropertyTypes is entered.
func (s *BaseGQLListener) EnterEdgeTypePropertyTypes(ctx *EdgeTypePropertyTypesContext) {}

// ExitEdgeTypePropertyTypes is called when production edgeTypePropertyTypes is exited.
func (s *BaseGQLListener) ExitEdgeTypePropertyTypes(ctx *EdgeTypePropertyTypesContext) {}

// EnterEdgeTypePatternDirected is called when production edgeTypePatternDirected is entered.
func (s *BaseGQLListener) EnterEdgeTypePatternDirected(ctx *EdgeTypePatternDirectedContext) {}

// ExitEdgeTypePatternDirected is called when production edgeTypePatternDirected is exited.
func (s *BaseGQLListener) ExitEdgeTypePatternDirected(ctx *EdgeTypePatternDirectedContext) {}

// EnterEdgeTypePatternPointingRight is called when production edgeTypePatternPointingRight is entered.
func (s *BaseGQLListener) EnterEdgeTypePatternPointingRight(ctx *EdgeTypePatternPointingRightContext) {
}

// ExitEdgeTypePatternPointingRight is called when production edgeTypePatternPointingRight is exited.
func (s *BaseGQLListener) ExitEdgeTypePatternPointingRight(ctx *EdgeTypePatternPointingRightContext) {
}

// EnterEdgeTypePatternPointingLeft is called when production edgeTypePatternPointingLeft is entered.
func (s *BaseGQLListener) EnterEdgeTypePatternPointingLeft(ctx *EdgeTypePatternPointingLeftContext) {}

// ExitEdgeTypePatternPointingLeft is called when production edgeTypePatternPointingLeft is exited.
func (s *BaseGQLListener) ExitEdgeTypePatternPointingLeft(ctx *EdgeTypePatternPointingLeftContext) {}

// EnterEdgeTypePatternUndirected is called when production edgeTypePatternUndirected is entered.
func (s *BaseGQLListener) EnterEdgeTypePatternUndirected(ctx *EdgeTypePatternUndirectedContext) {}

// ExitEdgeTypePatternUndirected is called when production edgeTypePatternUndirected is exited.
func (s *BaseGQLListener) ExitEdgeTypePatternUndirected(ctx *EdgeTypePatternUndirectedContext) {}

// EnterArcTypePointingRight is called when production arcTypePointingRight is entered.
func (s *BaseGQLListener) EnterArcTypePointingRight(ctx *ArcTypePointingRightContext) {}

// ExitArcTypePointingRight is called when production arcTypePointingRight is exited.
func (s *BaseGQLListener) ExitArcTypePointingRight(ctx *ArcTypePointingRightContext) {}

// EnterArcTypePointingLeft is called when production arcTypePointingLeft is entered.
func (s *BaseGQLListener) EnterArcTypePointingLeft(ctx *ArcTypePointingLeftContext) {}

// ExitArcTypePointingLeft is called when production arcTypePointingLeft is exited.
func (s *BaseGQLListener) ExitArcTypePointingLeft(ctx *ArcTypePointingLeftContext) {}

// EnterArcTypeUndirected is called when production arcTypeUndirected is entered.
func (s *BaseGQLListener) EnterArcTypeUndirected(ctx *ArcTypeUndirectedContext) {}

// ExitArcTypeUndirected is called when production arcTypeUndirected is exited.
func (s *BaseGQLListener) ExitArcTypeUndirected(ctx *ArcTypeUndirectedContext) {}

// EnterSourceNodeTypeReference is called when production sourceNodeTypeReference is entered.
func (s *BaseGQLListener) EnterSourceNodeTypeReference(ctx *SourceNodeTypeReferenceContext) {}

// ExitSourceNodeTypeReference is called when production sourceNodeTypeReference is exited.
func (s *BaseGQLListener) ExitSourceNodeTypeReference(ctx *SourceNodeTypeReferenceContext) {}

// EnterDestinationNodeTypeReference is called when production destinationNodeTypeReference is entered.
func (s *BaseGQLListener) EnterDestinationNodeTypeReference(ctx *DestinationNodeTypeReferenceContext) {
}

// ExitDestinationNodeTypeReference is called when production destinationNodeTypeReference is exited.
func (s *BaseGQLListener) ExitDestinationNodeTypeReference(ctx *DestinationNodeTypeReferenceContext) {
}

// EnterEdgeKind is called when production edgeKind is entered.
func (s *BaseGQLListener) EnterEdgeKind(ctx *EdgeKindContext) {}

// ExitEdgeKind is called when production edgeKind is exited.
func (s *BaseGQLListener) ExitEdgeKind(ctx *EdgeKindContext) {}

// EnterEndpointPairPhrase is called when production endpointPairPhrase is entered.
func (s *BaseGQLListener) EnterEndpointPairPhrase(ctx *EndpointPairPhraseContext) {}

// ExitEndpointPairPhrase is called when production endpointPairPhrase is exited.
func (s *BaseGQLListener) ExitEndpointPairPhrase(ctx *EndpointPairPhraseContext) {}

// EnterEndpointPair is called when production endpointPair is entered.
func (s *BaseGQLListener) EnterEndpointPair(ctx *EndpointPairContext) {}

// ExitEndpointPair is called when production endpointPair is exited.
func (s *BaseGQLListener) ExitEndpointPair(ctx *EndpointPairContext) {}

// EnterEndpointPairDirected is called when production endpointPairDirected is entered.
func (s *BaseGQLListener) EnterEndpointPairDirected(ctx *EndpointPairDirectedContext) {}

// ExitEndpointPairDirected is called when production endpointPairDirected is exited.
func (s *BaseGQLListener) ExitEndpointPairDirected(ctx *EndpointPairDirectedContext) {}

// EnterEndpointPairPointingRight is called when production endpointPairPointingRight is entered.
func (s *BaseGQLListener) EnterEndpointPairPointingRight(ctx *EndpointPairPointingRightContext) {}

// ExitEndpointPairPointingRight is called when production endpointPairPointingRight is exited.
func (s *BaseGQLListener) ExitEndpointPairPointingRight(ctx *EndpointPairPointingRightContext) {}

// EnterEndpointPairPointingLeft is called when production endpointPairPointingLeft is entered.
func (s *BaseGQLListener) EnterEndpointPairPointingLeft(ctx *EndpointPairPointingLeftContext) {}

// ExitEndpointPairPointingLeft is called when production endpointPairPointingLeft is exited.
func (s *BaseGQLListener) ExitEndpointPairPointingLeft(ctx *EndpointPairPointingLeftContext) {}

// EnterEndpointPairUndirected is called when production endpointPairUndirected is entered.
func (s *BaseGQLListener) EnterEndpointPairUndirected(ctx *EndpointPairUndirectedContext) {}

// ExitEndpointPairUndirected is called when production endpointPairUndirected is exited.
func (s *BaseGQLListener) ExitEndpointPairUndirected(ctx *EndpointPairUndirectedContext) {}

// EnterConnectorPointingRight is called when production connectorPointingRight is entered.
func (s *BaseGQLListener) EnterConnectorPointingRight(ctx *ConnectorPointingRightContext) {}

// ExitConnectorPointingRight is called when production connectorPointingRight is exited.
func (s *BaseGQLListener) ExitConnectorPointingRight(ctx *ConnectorPointingRightContext) {}

// EnterConnectorUndirected is called when production connectorUndirected is entered.
func (s *BaseGQLListener) EnterConnectorUndirected(ctx *ConnectorUndirectedContext) {}

// ExitConnectorUndirected is called when production connectorUndirected is exited.
func (s *BaseGQLListener) ExitConnectorUndirected(ctx *ConnectorUndirectedContext) {}

// EnterSourceNodeTypeAlias is called when production sourceNodeTypeAlias is entered.
func (s *BaseGQLListener) EnterSourceNodeTypeAlias(ctx *SourceNodeTypeAliasContext) {}

// ExitSourceNodeTypeAlias is called when production sourceNodeTypeAlias is exited.
func (s *BaseGQLListener) ExitSourceNodeTypeAlias(ctx *SourceNodeTypeAliasContext) {}

// EnterDestinationNodeTypeAlias is called when production destinationNodeTypeAlias is entered.
func (s *BaseGQLListener) EnterDestinationNodeTypeAlias(ctx *DestinationNodeTypeAliasContext) {}

// ExitDestinationNodeTypeAlias is called when production destinationNodeTypeAlias is exited.
func (s *BaseGQLListener) ExitDestinationNodeTypeAlias(ctx *DestinationNodeTypeAliasContext) {}

// EnterLabelSetPhrase is called when production labelSetPhrase is entered.
func (s *BaseGQLListener) EnterLabelSetPhrase(ctx *LabelSetPhraseContext) {}

// ExitLabelSetPhrase is called when production labelSetPhrase is exited.
func (s *BaseGQLListener) ExitLabelSetPhrase(ctx *LabelSetPhraseContext) {}

// EnterLabelSetSpecification is called when production labelSetSpecification is entered.
func (s *BaseGQLListener) EnterLabelSetSpecification(ctx *LabelSetSpecificationContext) {}

// ExitLabelSetSpecification is called when production labelSetSpecification is exited.
func (s *BaseGQLListener) ExitLabelSetSpecification(ctx *LabelSetSpecificationContext) {}

// EnterPropertyTypesSpecification is called when production propertyTypesSpecification is entered.
func (s *BaseGQLListener) EnterPropertyTypesSpecification(ctx *PropertyTypesSpecificationContext) {}

// ExitPropertyTypesSpecification is called when production propertyTypesSpecification is exited.
func (s *BaseGQLListener) ExitPropertyTypesSpecification(ctx *PropertyTypesSpecificationContext) {}

// EnterPropertyTypeList is called when production propertyTypeList is entered.
func (s *BaseGQLListener) EnterPropertyTypeList(ctx *PropertyTypeListContext) {}

// ExitPropertyTypeList is called when production propertyTypeList is exited.
func (s *BaseGQLListener) ExitPropertyTypeList(ctx *PropertyTypeListContext) {}

// EnterPropertyType is called when production propertyType is entered.
func (s *BaseGQLListener) EnterPropertyType(ctx *PropertyTypeContext) {}

// ExitPropertyType is called when production propertyType is exited.
func (s *BaseGQLListener) ExitPropertyType(ctx *PropertyTypeContext) {}

// EnterPropertyValueType is called when production propertyValueType is entered.
func (s *BaseGQLListener) EnterPropertyValueType(ctx *PropertyValueTypeContext) {}

// ExitPropertyValueType is called when production propertyValueType is exited.
func (s *BaseGQLListener) ExitPropertyValueType(ctx *PropertyValueTypeContext) {}

// EnterBindingTableType is called when production bindingTableType is entered.
func (s *BaseGQLListener) EnterBindingTableType(ctx *BindingTableTypeContext) {}

// ExitBindingTableType is called when production bindingTableType is exited.
func (s *BaseGQLListener) ExitBindingTableType(ctx *BindingTableTypeContext) {}

// EnterDynamicPropertyValueTypeLabel is called when production dynamicPropertyValueTypeLabel is entered.
func (s *BaseGQLListener) EnterDynamicPropertyValueTypeLabel(ctx *DynamicPropertyValueTypeLabelContext) {
}

// ExitDynamicPropertyValueTypeLabel is called when production dynamicPropertyValueTypeLabel is exited.
func (s *BaseGQLListener) ExitDynamicPropertyValueTypeLabel(ctx *DynamicPropertyValueTypeLabelContext) {
}

// EnterClosedDynamicUnionTypeAtl1 is called when production closedDynamicUnionTypeAtl1 is entered.
func (s *BaseGQLListener) EnterClosedDynamicUnionTypeAtl1(ctx *ClosedDynamicUnionTypeAtl1Context) {}

// ExitClosedDynamicUnionTypeAtl1 is called when production closedDynamicUnionTypeAtl1 is exited.
func (s *BaseGQLListener) ExitClosedDynamicUnionTypeAtl1(ctx *ClosedDynamicUnionTypeAtl1Context) {}

// EnterClosedDynamicUnionTypeAtl2 is called when production closedDynamicUnionTypeAtl2 is entered.
func (s *BaseGQLListener) EnterClosedDynamicUnionTypeAtl2(ctx *ClosedDynamicUnionTypeAtl2Context) {}

// ExitClosedDynamicUnionTypeAtl2 is called when production closedDynamicUnionTypeAtl2 is exited.
func (s *BaseGQLListener) ExitClosedDynamicUnionTypeAtl2(ctx *ClosedDynamicUnionTypeAtl2Context) {}

// EnterPathValueTypeLabel is called when production pathValueTypeLabel is entered.
func (s *BaseGQLListener) EnterPathValueTypeLabel(ctx *PathValueTypeLabelContext) {}

// ExitPathValueTypeLabel is called when production pathValueTypeLabel is exited.
func (s *BaseGQLListener) ExitPathValueTypeLabel(ctx *PathValueTypeLabelContext) {}

// EnterListValueTypeAlt3 is called when production listValueTypeAlt3 is entered.
func (s *BaseGQLListener) EnterListValueTypeAlt3(ctx *ListValueTypeAlt3Context) {}

// ExitListValueTypeAlt3 is called when production listValueTypeAlt3 is exited.
func (s *BaseGQLListener) ExitListValueTypeAlt3(ctx *ListValueTypeAlt3Context) {}

// EnterListValueTypeAlt2 is called when production listValueTypeAlt2 is entered.
func (s *BaseGQLListener) EnterListValueTypeAlt2(ctx *ListValueTypeAlt2Context) {}

// ExitListValueTypeAlt2 is called when production listValueTypeAlt2 is exited.
func (s *BaseGQLListener) ExitListValueTypeAlt2(ctx *ListValueTypeAlt2Context) {}

// EnterListValueTypeAlt1 is called when production listValueTypeAlt1 is entered.
func (s *BaseGQLListener) EnterListValueTypeAlt1(ctx *ListValueTypeAlt1Context) {}

// ExitListValueTypeAlt1 is called when production listValueTypeAlt1 is exited.
func (s *BaseGQLListener) ExitListValueTypeAlt1(ctx *ListValueTypeAlt1Context) {}

// EnterPredefinedTypeLabel is called when production predefinedTypeLabel is entered.
func (s *BaseGQLListener) EnterPredefinedTypeLabel(ctx *PredefinedTypeLabelContext) {}

// ExitPredefinedTypeLabel is called when production predefinedTypeLabel is exited.
func (s *BaseGQLListener) ExitPredefinedTypeLabel(ctx *PredefinedTypeLabelContext) {}

// EnterRecordTypeLabel is called when production recordTypeLabel is entered.
func (s *BaseGQLListener) EnterRecordTypeLabel(ctx *RecordTypeLabelContext) {}

// ExitRecordTypeLabel is called when production recordTypeLabel is exited.
func (s *BaseGQLListener) ExitRecordTypeLabel(ctx *RecordTypeLabelContext) {}

// EnterOpenDynamicUnionTypeLabel is called when production openDynamicUnionTypeLabel is entered.
func (s *BaseGQLListener) EnterOpenDynamicUnionTypeLabel(ctx *OpenDynamicUnionTypeLabelContext) {}

// ExitOpenDynamicUnionTypeLabel is called when production openDynamicUnionTypeLabel is exited.
func (s *BaseGQLListener) ExitOpenDynamicUnionTypeLabel(ctx *OpenDynamicUnionTypeLabelContext) {}

// EnterTyped is called when production typed is entered.
func (s *BaseGQLListener) EnterTyped(ctx *TypedContext) {}

// ExitTyped is called when production typed is exited.
func (s *BaseGQLListener) ExitTyped(ctx *TypedContext) {}

// EnterPredefinedType is called when production predefinedType is entered.
func (s *BaseGQLListener) EnterPredefinedType(ctx *PredefinedTypeContext) {}

// ExitPredefinedType is called when production predefinedType is exited.
func (s *BaseGQLListener) ExitPredefinedType(ctx *PredefinedTypeContext) {}

// EnterBooleanType is called when production booleanType is entered.
func (s *BaseGQLListener) EnterBooleanType(ctx *BooleanTypeContext) {}

// ExitBooleanType is called when production booleanType is exited.
func (s *BaseGQLListener) ExitBooleanType(ctx *BooleanTypeContext) {}

// EnterCharacterStringType is called when production characterStringType is entered.
func (s *BaseGQLListener) EnterCharacterStringType(ctx *CharacterStringTypeContext) {}

// ExitCharacterStringType is called when production characterStringType is exited.
func (s *BaseGQLListener) ExitCharacterStringType(ctx *CharacterStringTypeContext) {}

// EnterByteStringType is called when production byteStringType is entered.
func (s *BaseGQLListener) EnterByteStringType(ctx *ByteStringTypeContext) {}

// ExitByteStringType is called when production byteStringType is exited.
func (s *BaseGQLListener) ExitByteStringType(ctx *ByteStringTypeContext) {}

// EnterMinLength is called when production minLength is entered.
func (s *BaseGQLListener) EnterMinLength(ctx *MinLengthContext) {}

// ExitMinLength is called when production minLength is exited.
func (s *BaseGQLListener) ExitMinLength(ctx *MinLengthContext) {}

// EnterMaxLength is called when production maxLength is entered.
func (s *BaseGQLListener) EnterMaxLength(ctx *MaxLengthContext) {}

// ExitMaxLength is called when production maxLength is exited.
func (s *BaseGQLListener) ExitMaxLength(ctx *MaxLengthContext) {}

// EnterFixedLength is called when production fixedLength is entered.
func (s *BaseGQLListener) EnterFixedLength(ctx *FixedLengthContext) {}

// ExitFixedLength is called when production fixedLength is exited.
func (s *BaseGQLListener) ExitFixedLength(ctx *FixedLengthContext) {}

// EnterNumericType is called when production numericType is entered.
func (s *BaseGQLListener) EnterNumericType(ctx *NumericTypeContext) {}

// ExitNumericType is called when production numericType is exited.
func (s *BaseGQLListener) ExitNumericType(ctx *NumericTypeContext) {}

// EnterExactNumericType is called when production exactNumericType is entered.
func (s *BaseGQLListener) EnterExactNumericType(ctx *ExactNumericTypeContext) {}

// ExitExactNumericType is called when production exactNumericType is exited.
func (s *BaseGQLListener) ExitExactNumericType(ctx *ExactNumericTypeContext) {}

// EnterBinaryExactNumericType is called when production binaryExactNumericType is entered.
func (s *BaseGQLListener) EnterBinaryExactNumericType(ctx *BinaryExactNumericTypeContext) {}

// ExitBinaryExactNumericType is called when production binaryExactNumericType is exited.
func (s *BaseGQLListener) ExitBinaryExactNumericType(ctx *BinaryExactNumericTypeContext) {}

// EnterSignedBinaryExactNumericType is called when production signedBinaryExactNumericType is entered.
func (s *BaseGQLListener) EnterSignedBinaryExactNumericType(ctx *SignedBinaryExactNumericTypeContext) {
}

// ExitSignedBinaryExactNumericType is called when production signedBinaryExactNumericType is exited.
func (s *BaseGQLListener) ExitSignedBinaryExactNumericType(ctx *SignedBinaryExactNumericTypeContext) {
}

// EnterUnsignedBinaryExactNumericType is called when production unsignedBinaryExactNumericType is entered.
func (s *BaseGQLListener) EnterUnsignedBinaryExactNumericType(ctx *UnsignedBinaryExactNumericTypeContext) {
}

// ExitUnsignedBinaryExactNumericType is called when production unsignedBinaryExactNumericType is exited.
func (s *BaseGQLListener) ExitUnsignedBinaryExactNumericType(ctx *UnsignedBinaryExactNumericTypeContext) {
}

// EnterVerboseBinaryExactNumericType is called when production verboseBinaryExactNumericType is entered.
func (s *BaseGQLListener) EnterVerboseBinaryExactNumericType(ctx *VerboseBinaryExactNumericTypeContext) {
}

// ExitVerboseBinaryExactNumericType is called when production verboseBinaryExactNumericType is exited.
func (s *BaseGQLListener) ExitVerboseBinaryExactNumericType(ctx *VerboseBinaryExactNumericTypeContext) {
}

// EnterDecimalExactNumericType is called when production decimalExactNumericType is entered.
func (s *BaseGQLListener) EnterDecimalExactNumericType(ctx *DecimalExactNumericTypeContext) {}

// ExitDecimalExactNumericType is called when production decimalExactNumericType is exited.
func (s *BaseGQLListener) ExitDecimalExactNumericType(ctx *DecimalExactNumericTypeContext) {}

// EnterPrecision is called when production precision is entered.
func (s *BaseGQLListener) EnterPrecision(ctx *PrecisionContext) {}

// ExitPrecision is called when production precision is exited.
func (s *BaseGQLListener) ExitPrecision(ctx *PrecisionContext) {}

// EnterScale is called when production scale is entered.
func (s *BaseGQLListener) EnterScale(ctx *ScaleContext) {}

// ExitScale is called when production scale is exited.
func (s *BaseGQLListener) ExitScale(ctx *ScaleContext) {}

// EnterApproximateNumericType is called when production approximateNumericType is entered.
func (s *BaseGQLListener) EnterApproximateNumericType(ctx *ApproximateNumericTypeContext) {}

// ExitApproximateNumericType is called when production approximateNumericType is exited.
func (s *BaseGQLListener) ExitApproximateNumericType(ctx *ApproximateNumericTypeContext) {}

// EnterTemporalType is called when production temporalType is entered.
func (s *BaseGQLListener) EnterTemporalType(ctx *TemporalTypeContext) {}

// ExitTemporalType is called when production temporalType is exited.
func (s *BaseGQLListener) ExitTemporalType(ctx *TemporalTypeContext) {}

// EnterTemporalInstantType is called when production temporalInstantType is entered.
func (s *BaseGQLListener) EnterTemporalInstantType(ctx *TemporalInstantTypeContext) {}

// ExitTemporalInstantType is called when production temporalInstantType is exited.
func (s *BaseGQLListener) ExitTemporalInstantType(ctx *TemporalInstantTypeContext) {}

// EnterDatetimeType is called when production datetimeType is entered.
func (s *BaseGQLListener) EnterDatetimeType(ctx *DatetimeTypeContext) {}

// ExitDatetimeType is called when production datetimeType is exited.
func (s *BaseGQLListener) ExitDatetimeType(ctx *DatetimeTypeContext) {}

// EnterLocaldatetimeType is called when production localdatetimeType is entered.
func (s *BaseGQLListener) EnterLocaldatetimeType(ctx *LocaldatetimeTypeContext) {}

// ExitLocaldatetimeType is called when production localdatetimeType is exited.
func (s *BaseGQLListener) ExitLocaldatetimeType(ctx *LocaldatetimeTypeContext) {}

// EnterDateType is called when production dateType is entered.
func (s *BaseGQLListener) EnterDateType(ctx *DateTypeContext) {}

// ExitDateType is called when production dateType is exited.
func (s *BaseGQLListener) ExitDateType(ctx *DateTypeContext) {}

// EnterTimeType is called when production timeType is entered.
func (s *BaseGQLListener) EnterTimeType(ctx *TimeTypeContext) {}

// ExitTimeType is called when production timeType is exited.
func (s *BaseGQLListener) ExitTimeType(ctx *TimeTypeContext) {}

// EnterLocaltimeType is called when production localtimeType is entered.
func (s *BaseGQLListener) EnterLocaltimeType(ctx *LocaltimeTypeContext) {}

// ExitLocaltimeType is called when production localtimeType is exited.
func (s *BaseGQLListener) ExitLocaltimeType(ctx *LocaltimeTypeContext) {}

// EnterTemporalDurationType is called when production temporalDurationType is entered.
func (s *BaseGQLListener) EnterTemporalDurationType(ctx *TemporalDurationTypeContext) {}

// ExitTemporalDurationType is called when production temporalDurationType is exited.
func (s *BaseGQLListener) ExitTemporalDurationType(ctx *TemporalDurationTypeContext) {}

// EnterTemporalDurationQualifier is called when production temporalDurationQualifier is entered.
func (s *BaseGQLListener) EnterTemporalDurationQualifier(ctx *TemporalDurationQualifierContext) {}

// ExitTemporalDurationQualifier is called when production temporalDurationQualifier is exited.
func (s *BaseGQLListener) ExitTemporalDurationQualifier(ctx *TemporalDurationQualifierContext) {}

// EnterReferenceValueType is called when production referenceValueType is entered.
func (s *BaseGQLListener) EnterReferenceValueType(ctx *ReferenceValueTypeContext) {}

// ExitReferenceValueType is called when production referenceValueType is exited.
func (s *BaseGQLListener) ExitReferenceValueType(ctx *ReferenceValueTypeContext) {}

// EnterImmaterialValueType is called when production immaterialValueType is entered.
func (s *BaseGQLListener) EnterImmaterialValueType(ctx *ImmaterialValueTypeContext) {}

// ExitImmaterialValueType is called when production immaterialValueType is exited.
func (s *BaseGQLListener) ExitImmaterialValueType(ctx *ImmaterialValueTypeContext) {}

// EnterNullType is called when production nullType is entered.
func (s *BaseGQLListener) EnterNullType(ctx *NullTypeContext) {}

// ExitNullType is called when production nullType is exited.
func (s *BaseGQLListener) ExitNullType(ctx *NullTypeContext) {}

// EnterEmptyType is called when production emptyType is entered.
func (s *BaseGQLListener) EnterEmptyType(ctx *EmptyTypeContext) {}

// ExitEmptyType is called when production emptyType is exited.
func (s *BaseGQLListener) ExitEmptyType(ctx *EmptyTypeContext) {}

// EnterGraphReferenceValueType is called when production graphReferenceValueType is entered.
func (s *BaseGQLListener) EnterGraphReferenceValueType(ctx *GraphReferenceValueTypeContext) {}

// ExitGraphReferenceValueType is called when production graphReferenceValueType is exited.
func (s *BaseGQLListener) ExitGraphReferenceValueType(ctx *GraphReferenceValueTypeContext) {}

// EnterClosedGraphReferenceValueType is called when production closedGraphReferenceValueType is entered.
func (s *BaseGQLListener) EnterClosedGraphReferenceValueType(ctx *ClosedGraphReferenceValueTypeContext) {
}

// ExitClosedGraphReferenceValueType is called when production closedGraphReferenceValueType is exited.
func (s *BaseGQLListener) ExitClosedGraphReferenceValueType(ctx *ClosedGraphReferenceValueTypeContext) {
}

// EnterOpenGraphReferenceValueType is called when production openGraphReferenceValueType is entered.
func (s *BaseGQLListener) EnterOpenGraphReferenceValueType(ctx *OpenGraphReferenceValueTypeContext) {}

// ExitOpenGraphReferenceValueType is called when production openGraphReferenceValueType is exited.
func (s *BaseGQLListener) ExitOpenGraphReferenceValueType(ctx *OpenGraphReferenceValueTypeContext) {}

// EnterBindingTableReferenceValueType is called when production bindingTableReferenceValueType is entered.
func (s *BaseGQLListener) EnterBindingTableReferenceValueType(ctx *BindingTableReferenceValueTypeContext) {
}

// ExitBindingTableReferenceValueType is called when production bindingTableReferenceValueType is exited.
func (s *BaseGQLListener) ExitBindingTableReferenceValueType(ctx *BindingTableReferenceValueTypeContext) {
}

// EnterNodeReferenceValueType is called when production nodeReferenceValueType is entered.
func (s *BaseGQLListener) EnterNodeReferenceValueType(ctx *NodeReferenceValueTypeContext) {}

// ExitNodeReferenceValueType is called when production nodeReferenceValueType is exited.
func (s *BaseGQLListener) ExitNodeReferenceValueType(ctx *NodeReferenceValueTypeContext) {}

// EnterClosedNodeReferenceValueType is called when production closedNodeReferenceValueType is entered.
func (s *BaseGQLListener) EnterClosedNodeReferenceValueType(ctx *ClosedNodeReferenceValueTypeContext) {
}

// ExitClosedNodeReferenceValueType is called when production closedNodeReferenceValueType is exited.
func (s *BaseGQLListener) ExitClosedNodeReferenceValueType(ctx *ClosedNodeReferenceValueTypeContext) {
}

// EnterOpenNodeReferenceValueType is called when production openNodeReferenceValueType is entered.
func (s *BaseGQLListener) EnterOpenNodeReferenceValueType(ctx *OpenNodeReferenceValueTypeContext) {}

// ExitOpenNodeReferenceValueType is called when production openNodeReferenceValueType is exited.
func (s *BaseGQLListener) ExitOpenNodeReferenceValueType(ctx *OpenNodeReferenceValueTypeContext) {}

// EnterEdgeReferenceValueType is called when production edgeReferenceValueType is entered.
func (s *BaseGQLListener) EnterEdgeReferenceValueType(ctx *EdgeReferenceValueTypeContext) {}

// ExitEdgeReferenceValueType is called when production edgeReferenceValueType is exited.
func (s *BaseGQLListener) ExitEdgeReferenceValueType(ctx *EdgeReferenceValueTypeContext) {}

// EnterClosedEdgeReferenceValueType is called when production closedEdgeReferenceValueType is entered.
func (s *BaseGQLListener) EnterClosedEdgeReferenceValueType(ctx *ClosedEdgeReferenceValueTypeContext) {
}

// ExitClosedEdgeReferenceValueType is called when production closedEdgeReferenceValueType is exited.
func (s *BaseGQLListener) ExitClosedEdgeReferenceValueType(ctx *ClosedEdgeReferenceValueTypeContext) {
}

// EnterOpenEdgeReferenceValueType is called when production openEdgeReferenceValueType is entered.
func (s *BaseGQLListener) EnterOpenEdgeReferenceValueType(ctx *OpenEdgeReferenceValueTypeContext) {}

// ExitOpenEdgeReferenceValueType is called when production openEdgeReferenceValueType is exited.
func (s *BaseGQLListener) ExitOpenEdgeReferenceValueType(ctx *OpenEdgeReferenceValueTypeContext) {}

// EnterPathValueType is called when production pathValueType is entered.
func (s *BaseGQLListener) EnterPathValueType(ctx *PathValueTypeContext) {}

// ExitPathValueType is called when production pathValueType is exited.
func (s *BaseGQLListener) ExitPathValueType(ctx *PathValueTypeContext) {}

// EnterListValueTypeName is called when production listValueTypeName is entered.
func (s *BaseGQLListener) EnterListValueTypeName(ctx *ListValueTypeNameContext) {}

// ExitListValueTypeName is called when production listValueTypeName is exited.
func (s *BaseGQLListener) ExitListValueTypeName(ctx *ListValueTypeNameContext) {}

// EnterListValueTypeNameSynonym is called when production listValueTypeNameSynonym is entered.
func (s *BaseGQLListener) EnterListValueTypeNameSynonym(ctx *ListValueTypeNameSynonymContext) {}

// ExitListValueTypeNameSynonym is called when production listValueTypeNameSynonym is exited.
func (s *BaseGQLListener) ExitListValueTypeNameSynonym(ctx *ListValueTypeNameSynonymContext) {}

// EnterRecordType is called when production recordType is entered.
func (s *BaseGQLListener) EnterRecordType(ctx *RecordTypeContext) {}

// ExitRecordType is called when production recordType is exited.
func (s *BaseGQLListener) ExitRecordType(ctx *RecordTypeContext) {}

// EnterFieldTypesSpecification is called when production fieldTypesSpecification is entered.
func (s *BaseGQLListener) EnterFieldTypesSpecification(ctx *FieldTypesSpecificationContext) {}

// ExitFieldTypesSpecification is called when production fieldTypesSpecification is exited.
func (s *BaseGQLListener) ExitFieldTypesSpecification(ctx *FieldTypesSpecificationContext) {}

// EnterFieldTypeList is called when production fieldTypeList is entered.
func (s *BaseGQLListener) EnterFieldTypeList(ctx *FieldTypeListContext) {}

// ExitFieldTypeList is called when production fieldTypeList is exited.
func (s *BaseGQLListener) ExitFieldTypeList(ctx *FieldTypeListContext) {}

// EnterNotNull is called when production notNull is entered.
func (s *BaseGQLListener) EnterNotNull(ctx *NotNullContext) {}

// ExitNotNull is called when production notNull is exited.
func (s *BaseGQLListener) ExitNotNull(ctx *NotNullContext) {}

// EnterFieldType is called when production fieldType is entered.
func (s *BaseGQLListener) EnterFieldType(ctx *FieldTypeContext) {}

// ExitFieldType is called when production fieldType is exited.
func (s *BaseGQLListener) ExitFieldType(ctx *FieldTypeContext) {}

// EnterSearchCondition is called when production searchCondition is entered.
func (s *BaseGQLListener) EnterSearchCondition(ctx *SearchConditionContext) {}

// ExitSearchCondition is called when production searchCondition is exited.
func (s *BaseGQLListener) ExitSearchCondition(ctx *SearchConditionContext) {}

// EnterPredicate is called when production predicate is entered.
func (s *BaseGQLListener) EnterPredicate(ctx *PredicateContext) {}

// ExitPredicate is called when production predicate is exited.
func (s *BaseGQLListener) ExitPredicate(ctx *PredicateContext) {}

// EnterCompOp is called when production compOp is entered.
func (s *BaseGQLListener) EnterCompOp(ctx *CompOpContext) {}

// ExitCompOp is called when production compOp is exited.
func (s *BaseGQLListener) ExitCompOp(ctx *CompOpContext) {}

// EnterExistsPredicate is called when production existsPredicate is entered.
func (s *BaseGQLListener) EnterExistsPredicate(ctx *ExistsPredicateContext) {}

// ExitExistsPredicate is called when production existsPredicate is exited.
func (s *BaseGQLListener) ExitExistsPredicate(ctx *ExistsPredicateContext) {}

// EnterNullPredicate is called when production nullPredicate is entered.
func (s *BaseGQLListener) EnterNullPredicate(ctx *NullPredicateContext) {}

// ExitNullPredicate is called when production nullPredicate is exited.
func (s *BaseGQLListener) ExitNullPredicate(ctx *NullPredicateContext) {}

// EnterNullPredicatePart2 is called when production nullPredicatePart2 is entered.
func (s *BaseGQLListener) EnterNullPredicatePart2(ctx *NullPredicatePart2Context) {}

// ExitNullPredicatePart2 is called when production nullPredicatePart2 is exited.
func (s *BaseGQLListener) ExitNullPredicatePart2(ctx *NullPredicatePart2Context) {}

// EnterValueTypePredicate is called when production valueTypePredicate is entered.
func (s *BaseGQLListener) EnterValueTypePredicate(ctx *ValueTypePredicateContext) {}

// ExitValueTypePredicate is called when production valueTypePredicate is exited.
func (s *BaseGQLListener) ExitValueTypePredicate(ctx *ValueTypePredicateContext) {}

// EnterValueTypePredicatePart2 is called when production valueTypePredicatePart2 is entered.
func (s *BaseGQLListener) EnterValueTypePredicatePart2(ctx *ValueTypePredicatePart2Context) {}

// ExitValueTypePredicatePart2 is called when production valueTypePredicatePart2 is exited.
func (s *BaseGQLListener) ExitValueTypePredicatePart2(ctx *ValueTypePredicatePart2Context) {}

// EnterNormalizedPredicatePart2 is called when production normalizedPredicatePart2 is entered.
func (s *BaseGQLListener) EnterNormalizedPredicatePart2(ctx *NormalizedPredicatePart2Context) {}

// ExitNormalizedPredicatePart2 is called when production normalizedPredicatePart2 is exited.
func (s *BaseGQLListener) ExitNormalizedPredicatePart2(ctx *NormalizedPredicatePart2Context) {}

// EnterDirectedPredicate is called when production directedPredicate is entered.
func (s *BaseGQLListener) EnterDirectedPredicate(ctx *DirectedPredicateContext) {}

// ExitDirectedPredicate is called when production directedPredicate is exited.
func (s *BaseGQLListener) ExitDirectedPredicate(ctx *DirectedPredicateContext) {}

// EnterDirectedPredicatePart2 is called when production directedPredicatePart2 is entered.
func (s *BaseGQLListener) EnterDirectedPredicatePart2(ctx *DirectedPredicatePart2Context) {}

// ExitDirectedPredicatePart2 is called when production directedPredicatePart2 is exited.
func (s *BaseGQLListener) ExitDirectedPredicatePart2(ctx *DirectedPredicatePart2Context) {}

// EnterLabeledPredicate is called when production labeledPredicate is entered.
func (s *BaseGQLListener) EnterLabeledPredicate(ctx *LabeledPredicateContext) {}

// ExitLabeledPredicate is called when production labeledPredicate is exited.
func (s *BaseGQLListener) ExitLabeledPredicate(ctx *LabeledPredicateContext) {}

// EnterLabeledPredicatePart2 is called when production labeledPredicatePart2 is entered.
func (s *BaseGQLListener) EnterLabeledPredicatePart2(ctx *LabeledPredicatePart2Context) {}

// ExitLabeledPredicatePart2 is called when production labeledPredicatePart2 is exited.
func (s *BaseGQLListener) ExitLabeledPredicatePart2(ctx *LabeledPredicatePart2Context) {}

// EnterIsLabeledOrColon is called when production isLabeledOrColon is entered.
func (s *BaseGQLListener) EnterIsLabeledOrColon(ctx *IsLabeledOrColonContext) {}

// ExitIsLabeledOrColon is called when production isLabeledOrColon is exited.
func (s *BaseGQLListener) ExitIsLabeledOrColon(ctx *IsLabeledOrColonContext) {}

// EnterSourceDestinationPredicate is called when production sourceDestinationPredicate is entered.
func (s *BaseGQLListener) EnterSourceDestinationPredicate(ctx *SourceDestinationPredicateContext) {}

// ExitSourceDestinationPredicate is called when production sourceDestinationPredicate is exited.
func (s *BaseGQLListener) ExitSourceDestinationPredicate(ctx *SourceDestinationPredicateContext) {}

// EnterNodeReference is called when production nodeReference is entered.
func (s *BaseGQLListener) EnterNodeReference(ctx *NodeReferenceContext) {}

// ExitNodeReference is called when production nodeReference is exited.
func (s *BaseGQLListener) ExitNodeReference(ctx *NodeReferenceContext) {}

// EnterSourcePredicatePart2 is called when production sourcePredicatePart2 is entered.
func (s *BaseGQLListener) EnterSourcePredicatePart2(ctx *SourcePredicatePart2Context) {}

// ExitSourcePredicatePart2 is called when production sourcePredicatePart2 is exited.
func (s *BaseGQLListener) ExitSourcePredicatePart2(ctx *SourcePredicatePart2Context) {}

// EnterDestinationPredicatePart2 is called when production destinationPredicatePart2 is entered.
func (s *BaseGQLListener) EnterDestinationPredicatePart2(ctx *DestinationPredicatePart2Context) {}

// ExitDestinationPredicatePart2 is called when production destinationPredicatePart2 is exited.
func (s *BaseGQLListener) ExitDestinationPredicatePart2(ctx *DestinationPredicatePart2Context) {}

// EnterEdgeReference is called when production edgeReference is entered.
func (s *BaseGQLListener) EnterEdgeReference(ctx *EdgeReferenceContext) {}

// ExitEdgeReference is called when production edgeReference is exited.
func (s *BaseGQLListener) ExitEdgeReference(ctx *EdgeReferenceContext) {}

// EnterAll_differentPredicate is called when production all_differentPredicate is entered.
func (s *BaseGQLListener) EnterAll_differentPredicate(ctx *All_differentPredicateContext) {}

// ExitAll_differentPredicate is called when production all_differentPredicate is exited.
func (s *BaseGQLListener) ExitAll_differentPredicate(ctx *All_differentPredicateContext) {}

// EnterSamePredicate is called when production samePredicate is entered.
func (s *BaseGQLListener) EnterSamePredicate(ctx *SamePredicateContext) {}

// ExitSamePredicate is called when production samePredicate is exited.
func (s *BaseGQLListener) ExitSamePredicate(ctx *SamePredicateContext) {}

// EnterProperty_existsPredicate is called when production property_existsPredicate is entered.
func (s *BaseGQLListener) EnterProperty_existsPredicate(ctx *Property_existsPredicateContext) {}

// ExitProperty_existsPredicate is called when production property_existsPredicate is exited.
func (s *BaseGQLListener) ExitProperty_existsPredicate(ctx *Property_existsPredicateContext) {}

// EnterConjunctiveExprAlt is called when production conjunctiveExprAlt is entered.
func (s *BaseGQLListener) EnterConjunctiveExprAlt(ctx *ConjunctiveExprAltContext) {}

// ExitConjunctiveExprAlt is called when production conjunctiveExprAlt is exited.
func (s *BaseGQLListener) ExitConjunctiveExprAlt(ctx *ConjunctiveExprAltContext) {}

// EnterPropertyGraphExprAlt is called when production propertyGraphExprAlt is entered.
func (s *BaseGQLListener) EnterPropertyGraphExprAlt(ctx *PropertyGraphExprAltContext) {}

// ExitPropertyGraphExprAlt is called when production propertyGraphExprAlt is exited.
func (s *BaseGQLListener) ExitPropertyGraphExprAlt(ctx *PropertyGraphExprAltContext) {}

// EnterMultDivExprAlt is called when production multDivExprAlt is entered.
func (s *BaseGQLListener) EnterMultDivExprAlt(ctx *MultDivExprAltContext) {}

// ExitMultDivExprAlt is called when production multDivExprAlt is exited.
func (s *BaseGQLListener) ExitMultDivExprAlt(ctx *MultDivExprAltContext) {}

// EnterBindingTableExprAlt is called when production bindingTableExprAlt is entered.
func (s *BaseGQLListener) EnterBindingTableExprAlt(ctx *BindingTableExprAltContext) {}

// ExitBindingTableExprAlt is called when production bindingTableExprAlt is exited.
func (s *BaseGQLListener) ExitBindingTableExprAlt(ctx *BindingTableExprAltContext) {}

// EnterSignedExprAlt is called when production signedExprAlt is entered.
func (s *BaseGQLListener) EnterSignedExprAlt(ctx *SignedExprAltContext) {}

// ExitSignedExprAlt is called when production signedExprAlt is exited.
func (s *BaseGQLListener) ExitSignedExprAlt(ctx *SignedExprAltContext) {}

// EnterIsNotExprAlt is called when production isNotExprAlt is entered.
func (s *BaseGQLListener) EnterIsNotExprAlt(ctx *IsNotExprAltContext) {}

// ExitIsNotExprAlt is called when production isNotExprAlt is exited.
func (s *BaseGQLListener) ExitIsNotExprAlt(ctx *IsNotExprAltContext) {}

// EnterNormalizedPredicateExprAlt is called when production normalizedPredicateExprAlt is entered.
func (s *BaseGQLListener) EnterNormalizedPredicateExprAlt(ctx *NormalizedPredicateExprAltContext) {}

// ExitNormalizedPredicateExprAlt is called when production normalizedPredicateExprAlt is exited.
func (s *BaseGQLListener) ExitNormalizedPredicateExprAlt(ctx *NormalizedPredicateExprAltContext) {}

// EnterNotExprAlt is called when production notExprAlt is entered.
func (s *BaseGQLListener) EnterNotExprAlt(ctx *NotExprAltContext) {}

// ExitNotExprAlt is called when production notExprAlt is exited.
func (s *BaseGQLListener) ExitNotExprAlt(ctx *NotExprAltContext) {}

// EnterValueFunctionExprAlt is called when production valueFunctionExprAlt is entered.
func (s *BaseGQLListener) EnterValueFunctionExprAlt(ctx *ValueFunctionExprAltContext) {}

// ExitValueFunctionExprAlt is called when production valueFunctionExprAlt is exited.
func (s *BaseGQLListener) ExitValueFunctionExprAlt(ctx *ValueFunctionExprAltContext) {}

// EnterConcatenationExprAlt is called when production concatenationExprAlt is entered.
func (s *BaseGQLListener) EnterConcatenationExprAlt(ctx *ConcatenationExprAltContext) {}

// ExitConcatenationExprAlt is called when production concatenationExprAlt is exited.
func (s *BaseGQLListener) ExitConcatenationExprAlt(ctx *ConcatenationExprAltContext) {}

// EnterDisjunctiveExprAlt is called when production disjunctiveExprAlt is entered.
func (s *BaseGQLListener) EnterDisjunctiveExprAlt(ctx *DisjunctiveExprAltContext) {}

// ExitDisjunctiveExprAlt is called when production disjunctiveExprAlt is exited.
func (s *BaseGQLListener) ExitDisjunctiveExprAlt(ctx *DisjunctiveExprAltContext) {}

// EnterComparisonExprAlt is called when production comparisonExprAlt is entered.
func (s *BaseGQLListener) EnterComparisonExprAlt(ctx *ComparisonExprAltContext) {}

// ExitComparisonExprAlt is called when production comparisonExprAlt is exited.
func (s *BaseGQLListener) ExitComparisonExprAlt(ctx *ComparisonExprAltContext) {}

// EnterPrimaryExprAlt is called when production primaryExprAlt is entered.
func (s *BaseGQLListener) EnterPrimaryExprAlt(ctx *PrimaryExprAltContext) {}

// ExitPrimaryExprAlt is called when production primaryExprAlt is exited.
func (s *BaseGQLListener) ExitPrimaryExprAlt(ctx *PrimaryExprAltContext) {}

// EnterAddSubtractExprAlt is called when production addSubtractExprAlt is entered.
func (s *BaseGQLListener) EnterAddSubtractExprAlt(ctx *AddSubtractExprAltContext) {}

// ExitAddSubtractExprAlt is called when production addSubtractExprAlt is exited.
func (s *BaseGQLListener) ExitAddSubtractExprAlt(ctx *AddSubtractExprAltContext) {}

// EnterPredicateExprAlt is called when production predicateExprAlt is entered.
func (s *BaseGQLListener) EnterPredicateExprAlt(ctx *PredicateExprAltContext) {}

// ExitPredicateExprAlt is called when production predicateExprAlt is exited.
func (s *BaseGQLListener) ExitPredicateExprAlt(ctx *PredicateExprAltContext) {}

// EnterValueFunction is called when production valueFunction is entered.
func (s *BaseGQLListener) EnterValueFunction(ctx *ValueFunctionContext) {}

// ExitValueFunction is called when production valueFunction is exited.
func (s *BaseGQLListener) ExitValueFunction(ctx *ValueFunctionContext) {}

// EnterBooleanValueExpression is called when production booleanValueExpression is entered.
func (s *BaseGQLListener) EnterBooleanValueExpression(ctx *BooleanValueExpressionContext) {}

// ExitBooleanValueExpression is called when production booleanValueExpression is exited.
func (s *BaseGQLListener) ExitBooleanValueExpression(ctx *BooleanValueExpressionContext) {}

// EnterCharacterOrByteStringFunction is called when production characterOrByteStringFunction is entered.
func (s *BaseGQLListener) EnterCharacterOrByteStringFunction(ctx *CharacterOrByteStringFunctionContext) {
}

// ExitCharacterOrByteStringFunction is called when production characterOrByteStringFunction is exited.
func (s *BaseGQLListener) ExitCharacterOrByteStringFunction(ctx *CharacterOrByteStringFunctionContext) {
}

// EnterSubCharacterOrByteString is called when production subCharacterOrByteString is entered.
func (s *BaseGQLListener) EnterSubCharacterOrByteString(ctx *SubCharacterOrByteStringContext) {}

// ExitSubCharacterOrByteString is called when production subCharacterOrByteString is exited.
func (s *BaseGQLListener) ExitSubCharacterOrByteString(ctx *SubCharacterOrByteStringContext) {}

// EnterTrimSingleCharacterOrByteString is called when production trimSingleCharacterOrByteString is entered.
func (s *BaseGQLListener) EnterTrimSingleCharacterOrByteString(ctx *TrimSingleCharacterOrByteStringContext) {
}

// ExitTrimSingleCharacterOrByteString is called when production trimSingleCharacterOrByteString is exited.
func (s *BaseGQLListener) ExitTrimSingleCharacterOrByteString(ctx *TrimSingleCharacterOrByteStringContext) {
}

// EnterFoldCharacterString is called when production foldCharacterString is entered.
func (s *BaseGQLListener) EnterFoldCharacterString(ctx *FoldCharacterStringContext) {}

// ExitFoldCharacterString is called when production foldCharacterString is exited.
func (s *BaseGQLListener) ExitFoldCharacterString(ctx *FoldCharacterStringContext) {}

// EnterTrimMultiCharacterCharacterString is called when production trimMultiCharacterCharacterString is entered.
func (s *BaseGQLListener) EnterTrimMultiCharacterCharacterString(ctx *TrimMultiCharacterCharacterStringContext) {
}

// ExitTrimMultiCharacterCharacterString is called when production trimMultiCharacterCharacterString is exited.
func (s *BaseGQLListener) ExitTrimMultiCharacterCharacterString(ctx *TrimMultiCharacterCharacterStringContext) {
}

// EnterNormalizeCharacterString is called when production normalizeCharacterString is entered.
func (s *BaseGQLListener) EnterNormalizeCharacterString(ctx *NormalizeCharacterStringContext) {}

// ExitNormalizeCharacterString is called when production normalizeCharacterString is exited.
func (s *BaseGQLListener) ExitNormalizeCharacterString(ctx *NormalizeCharacterStringContext) {}

// EnterNodeReferenceValueExpression is called when production nodeReferenceValueExpression is entered.
func (s *BaseGQLListener) EnterNodeReferenceValueExpression(ctx *NodeReferenceValueExpressionContext) {
}

// ExitNodeReferenceValueExpression is called when production nodeReferenceValueExpression is exited.
func (s *BaseGQLListener) ExitNodeReferenceValueExpression(ctx *NodeReferenceValueExpressionContext) {
}

// EnterEdgeReferenceValueExpression is called when production edgeReferenceValueExpression is entered.
func (s *BaseGQLListener) EnterEdgeReferenceValueExpression(ctx *EdgeReferenceValueExpressionContext) {
}

// ExitEdgeReferenceValueExpression is called when production edgeReferenceValueExpression is exited.
func (s *BaseGQLListener) ExitEdgeReferenceValueExpression(ctx *EdgeReferenceValueExpressionContext) {
}

// EnterAggregatingValueExpression is called when production aggregatingValueExpression is entered.
func (s *BaseGQLListener) EnterAggregatingValueExpression(ctx *AggregatingValueExpressionContext) {}

// ExitAggregatingValueExpression is called when production aggregatingValueExpression is exited.
func (s *BaseGQLListener) ExitAggregatingValueExpression(ctx *AggregatingValueExpressionContext) {}

// EnterValueExpressionPrimary is called when production valueExpressionPrimary is entered.
func (s *BaseGQLListener) EnterValueExpressionPrimary(ctx *ValueExpressionPrimaryContext) {}

// ExitValueExpressionPrimary is called when production valueExpressionPrimary is exited.
func (s *BaseGQLListener) ExitValueExpressionPrimary(ctx *ValueExpressionPrimaryContext) {}

// EnterParenthesizedValueExpression is called when production parenthesizedValueExpression is entered.
func (s *BaseGQLListener) EnterParenthesizedValueExpression(ctx *ParenthesizedValueExpressionContext) {
}

// ExitParenthesizedValueExpression is called when production parenthesizedValueExpression is exited.
func (s *BaseGQLListener) ExitParenthesizedValueExpression(ctx *ParenthesizedValueExpressionContext) {
}

// EnterNonParenthesizedValueExpressionPrimary is called when production nonParenthesizedValueExpressionPrimary is entered.
func (s *BaseGQLListener) EnterNonParenthesizedValueExpressionPrimary(ctx *NonParenthesizedValueExpressionPrimaryContext) {
}

// ExitNonParenthesizedValueExpressionPrimary is called when production nonParenthesizedValueExpressionPrimary is exited.
func (s *BaseGQLListener) ExitNonParenthesizedValueExpressionPrimary(ctx *NonParenthesizedValueExpressionPrimaryContext) {
}

// EnterNonParenthesizedValueExpressionPrimarySpecialCase is called when production nonParenthesizedValueExpressionPrimarySpecialCase is entered.
func (s *BaseGQLListener) EnterNonParenthesizedValueExpressionPrimarySpecialCase(ctx *NonParenthesizedValueExpressionPrimarySpecialCaseContext) {
}

// ExitNonParenthesizedValueExpressionPrimarySpecialCase is called when production nonParenthesizedValueExpressionPrimarySpecialCase is exited.
func (s *BaseGQLListener) ExitNonParenthesizedValueExpressionPrimarySpecialCase(ctx *NonParenthesizedValueExpressionPrimarySpecialCaseContext) {
}

// EnterUnsignedValueSpecification is called when production unsignedValueSpecification is entered.
func (s *BaseGQLListener) EnterUnsignedValueSpecification(ctx *UnsignedValueSpecificationContext) {}

// ExitUnsignedValueSpecification is called when production unsignedValueSpecification is exited.
func (s *BaseGQLListener) ExitUnsignedValueSpecification(ctx *UnsignedValueSpecificationContext) {}

// EnterNonNegativeIntegerSpecification is called when production nonNegativeIntegerSpecification is entered.
func (s *BaseGQLListener) EnterNonNegativeIntegerSpecification(ctx *NonNegativeIntegerSpecificationContext) {
}

// ExitNonNegativeIntegerSpecification is called when production nonNegativeIntegerSpecification is exited.
func (s *BaseGQLListener) ExitNonNegativeIntegerSpecification(ctx *NonNegativeIntegerSpecificationContext) {
}

// EnterGeneralValueSpecification is called when production generalValueSpecification is entered.
func (s *BaseGQLListener) EnterGeneralValueSpecification(ctx *GeneralValueSpecificationContext) {}

// ExitGeneralValueSpecification is called when production generalValueSpecification is exited.
func (s *BaseGQLListener) ExitGeneralValueSpecification(ctx *GeneralValueSpecificationContext) {}

// EnterDynamicParameterSpecification is called when production dynamicParameterSpecification is entered.
func (s *BaseGQLListener) EnterDynamicParameterSpecification(ctx *DynamicParameterSpecificationContext) {
}

// ExitDynamicParameterSpecification is called when production dynamicParameterSpecification is exited.
func (s *BaseGQLListener) ExitDynamicParameterSpecification(ctx *DynamicParameterSpecificationContext) {
}

// EnterLetValueExpression is called when production letValueExpression is entered.
func (s *BaseGQLListener) EnterLetValueExpression(ctx *LetValueExpressionContext) {}

// ExitLetValueExpression is called when production letValueExpression is exited.
func (s *BaseGQLListener) ExitLetValueExpression(ctx *LetValueExpressionContext) {}

// EnterValueQueryExpression is called when production valueQueryExpression is entered.
func (s *BaseGQLListener) EnterValueQueryExpression(ctx *ValueQueryExpressionContext) {}

// ExitValueQueryExpression is called when production valueQueryExpression is exited.
func (s *BaseGQLListener) ExitValueQueryExpression(ctx *ValueQueryExpressionContext) {}

// EnterCaseExpression is called when production caseExpression is entered.
func (s *BaseGQLListener) EnterCaseExpression(ctx *CaseExpressionContext) {}

// ExitCaseExpression is called when production caseExpression is exited.
func (s *BaseGQLListener) ExitCaseExpression(ctx *CaseExpressionContext) {}

// EnterCaseAbbreviation is called when production caseAbbreviation is entered.
func (s *BaseGQLListener) EnterCaseAbbreviation(ctx *CaseAbbreviationContext) {}

// ExitCaseAbbreviation is called when production caseAbbreviation is exited.
func (s *BaseGQLListener) ExitCaseAbbreviation(ctx *CaseAbbreviationContext) {}

// EnterCaseSpecification is called when production caseSpecification is entered.
func (s *BaseGQLListener) EnterCaseSpecification(ctx *CaseSpecificationContext) {}

// ExitCaseSpecification is called when production caseSpecification is exited.
func (s *BaseGQLListener) ExitCaseSpecification(ctx *CaseSpecificationContext) {}

// EnterSimpleCase is called when production simpleCase is entered.
func (s *BaseGQLListener) EnterSimpleCase(ctx *SimpleCaseContext) {}

// ExitSimpleCase is called when production simpleCase is exited.
func (s *BaseGQLListener) ExitSimpleCase(ctx *SimpleCaseContext) {}

// EnterSearchedCase is called when production searchedCase is entered.
func (s *BaseGQLListener) EnterSearchedCase(ctx *SearchedCaseContext) {}

// ExitSearchedCase is called when production searchedCase is exited.
func (s *BaseGQLListener) ExitSearchedCase(ctx *SearchedCaseContext) {}

// EnterSimpleWhenClause is called when production simpleWhenClause is entered.
func (s *BaseGQLListener) EnterSimpleWhenClause(ctx *SimpleWhenClauseContext) {}

// ExitSimpleWhenClause is called when production simpleWhenClause is exited.
func (s *BaseGQLListener) ExitSimpleWhenClause(ctx *SimpleWhenClauseContext) {}

// EnterSearchedWhenClause is called when production searchedWhenClause is entered.
func (s *BaseGQLListener) EnterSearchedWhenClause(ctx *SearchedWhenClauseContext) {}

// ExitSearchedWhenClause is called when production searchedWhenClause is exited.
func (s *BaseGQLListener) ExitSearchedWhenClause(ctx *SearchedWhenClauseContext) {}

// EnterElseClause is called when production elseClause is entered.
func (s *BaseGQLListener) EnterElseClause(ctx *ElseClauseContext) {}

// ExitElseClause is called when production elseClause is exited.
func (s *BaseGQLListener) ExitElseClause(ctx *ElseClauseContext) {}

// EnterCaseOperand is called when production caseOperand is entered.
func (s *BaseGQLListener) EnterCaseOperand(ctx *CaseOperandContext) {}

// ExitCaseOperand is called when production caseOperand is exited.
func (s *BaseGQLListener) ExitCaseOperand(ctx *CaseOperandContext) {}

// EnterWhenOperandList is called when production whenOperandList is entered.
func (s *BaseGQLListener) EnterWhenOperandList(ctx *WhenOperandListContext) {}

// ExitWhenOperandList is called when production whenOperandList is exited.
func (s *BaseGQLListener) ExitWhenOperandList(ctx *WhenOperandListContext) {}

// EnterWhenOperand is called when production whenOperand is entered.
func (s *BaseGQLListener) EnterWhenOperand(ctx *WhenOperandContext) {}

// ExitWhenOperand is called when production whenOperand is exited.
func (s *BaseGQLListener) ExitWhenOperand(ctx *WhenOperandContext) {}

// EnterResult is called when production result is entered.
func (s *BaseGQLListener) EnterResult(ctx *ResultContext) {}

// ExitResult is called when production result is exited.
func (s *BaseGQLListener) ExitResult(ctx *ResultContext) {}

// EnterResultExpression is called when production resultExpression is entered.
func (s *BaseGQLListener) EnterResultExpression(ctx *ResultExpressionContext) {}

// ExitResultExpression is called when production resultExpression is exited.
func (s *BaseGQLListener) ExitResultExpression(ctx *ResultExpressionContext) {}

// EnterCastSpecification is called when production castSpecification is entered.
func (s *BaseGQLListener) EnterCastSpecification(ctx *CastSpecificationContext) {}

// ExitCastSpecification is called when production castSpecification is exited.
func (s *BaseGQLListener) ExitCastSpecification(ctx *CastSpecificationContext) {}

// EnterCastOperand is called when production castOperand is entered.
func (s *BaseGQLListener) EnterCastOperand(ctx *CastOperandContext) {}

// ExitCastOperand is called when production castOperand is exited.
func (s *BaseGQLListener) ExitCastOperand(ctx *CastOperandContext) {}

// EnterCastTarget is called when production castTarget is entered.
func (s *BaseGQLListener) EnterCastTarget(ctx *CastTargetContext) {}

// ExitCastTarget is called when production castTarget is exited.
func (s *BaseGQLListener) ExitCastTarget(ctx *CastTargetContext) {}

// EnterAggregateFunction is called when production aggregateFunction is entered.
func (s *BaseGQLListener) EnterAggregateFunction(ctx *AggregateFunctionContext) {}

// ExitAggregateFunction is called when production aggregateFunction is exited.
func (s *BaseGQLListener) ExitAggregateFunction(ctx *AggregateFunctionContext) {}

// EnterGeneralSetFunction is called when production generalSetFunction is entered.
func (s *BaseGQLListener) EnterGeneralSetFunction(ctx *GeneralSetFunctionContext) {}

// ExitGeneralSetFunction is called when production generalSetFunction is exited.
func (s *BaseGQLListener) ExitGeneralSetFunction(ctx *GeneralSetFunctionContext) {}

// EnterBinarySetFunction is called when production binarySetFunction is entered.
func (s *BaseGQLListener) EnterBinarySetFunction(ctx *BinarySetFunctionContext) {}

// ExitBinarySetFunction is called when production binarySetFunction is exited.
func (s *BaseGQLListener) ExitBinarySetFunction(ctx *BinarySetFunctionContext) {}

// EnterGeneralSetFunctionType is called when production generalSetFunctionType is entered.
func (s *BaseGQLListener) EnterGeneralSetFunctionType(ctx *GeneralSetFunctionTypeContext) {}

// ExitGeneralSetFunctionType is called when production generalSetFunctionType is exited.
func (s *BaseGQLListener) ExitGeneralSetFunctionType(ctx *GeneralSetFunctionTypeContext) {}

// EnterSetQuantifier is called when production setQuantifier is entered.
func (s *BaseGQLListener) EnterSetQuantifier(ctx *SetQuantifierContext) {}

// ExitSetQuantifier is called when production setQuantifier is exited.
func (s *BaseGQLListener) ExitSetQuantifier(ctx *SetQuantifierContext) {}

// EnterBinarySetFunctionType is called when production binarySetFunctionType is entered.
func (s *BaseGQLListener) EnterBinarySetFunctionType(ctx *BinarySetFunctionTypeContext) {}

// ExitBinarySetFunctionType is called when production binarySetFunctionType is exited.
func (s *BaseGQLListener) ExitBinarySetFunctionType(ctx *BinarySetFunctionTypeContext) {}

// EnterDependentValueExpression is called when production dependentValueExpression is entered.
func (s *BaseGQLListener) EnterDependentValueExpression(ctx *DependentValueExpressionContext) {}

// ExitDependentValueExpression is called when production dependentValueExpression is exited.
func (s *BaseGQLListener) ExitDependentValueExpression(ctx *DependentValueExpressionContext) {}

// EnterIndependentValueExpression is called when production independentValueExpression is entered.
func (s *BaseGQLListener) EnterIndependentValueExpression(ctx *IndependentValueExpressionContext) {}

// ExitIndependentValueExpression is called when production independentValueExpression is exited.
func (s *BaseGQLListener) ExitIndependentValueExpression(ctx *IndependentValueExpressionContext) {}

// EnterElement_idFunction is called when production element_idFunction is entered.
func (s *BaseGQLListener) EnterElement_idFunction(ctx *Element_idFunctionContext) {}

// ExitElement_idFunction is called when production element_idFunction is exited.
func (s *BaseGQLListener) ExitElement_idFunction(ctx *Element_idFunctionContext) {}

// EnterBindingVariableReference is called when production bindingVariableReference is entered.
func (s *BaseGQLListener) EnterBindingVariableReference(ctx *BindingVariableReferenceContext) {}

// ExitBindingVariableReference is called when production bindingVariableReference is exited.
func (s *BaseGQLListener) ExitBindingVariableReference(ctx *BindingVariableReferenceContext) {}

// EnterPathValueExpression is called when production pathValueExpression is entered.
func (s *BaseGQLListener) EnterPathValueExpression(ctx *PathValueExpressionContext) {}

// ExitPathValueExpression is called when production pathValueExpression is exited.
func (s *BaseGQLListener) ExitPathValueExpression(ctx *PathValueExpressionContext) {}

// EnterPathValueConstructor is called when production pathValueConstructor is entered.
func (s *BaseGQLListener) EnterPathValueConstructor(ctx *PathValueConstructorContext) {}

// ExitPathValueConstructor is called when production pathValueConstructor is exited.
func (s *BaseGQLListener) ExitPathValueConstructor(ctx *PathValueConstructorContext) {}

// EnterPathValueConstructorByEnumeration is called when production pathValueConstructorByEnumeration is entered.
func (s *BaseGQLListener) EnterPathValueConstructorByEnumeration(ctx *PathValueConstructorByEnumerationContext) {
}

// ExitPathValueConstructorByEnumeration is called when production pathValueConstructorByEnumeration is exited.
func (s *BaseGQLListener) ExitPathValueConstructorByEnumeration(ctx *PathValueConstructorByEnumerationContext) {
}

// EnterPathElementList is called when production pathElementList is entered.
func (s *BaseGQLListener) EnterPathElementList(ctx *PathElementListContext) {}

// ExitPathElementList is called when production pathElementList is exited.
func (s *BaseGQLListener) ExitPathElementList(ctx *PathElementListContext) {}

// EnterPathElementListStart is called when production pathElementListStart is entered.
func (s *BaseGQLListener) EnterPathElementListStart(ctx *PathElementListStartContext) {}

// ExitPathElementListStart is called when production pathElementListStart is exited.
func (s *BaseGQLListener) ExitPathElementListStart(ctx *PathElementListStartContext) {}

// EnterPathElementListStep is called when production pathElementListStep is entered.
func (s *BaseGQLListener) EnterPathElementListStep(ctx *PathElementListStepContext) {}

// ExitPathElementListStep is called when production pathElementListStep is exited.
func (s *BaseGQLListener) ExitPathElementListStep(ctx *PathElementListStepContext) {}

// EnterListValueExpression is called when production listValueExpression is entered.
func (s *BaseGQLListener) EnterListValueExpression(ctx *ListValueExpressionContext) {}

// ExitListValueExpression is called when production listValueExpression is exited.
func (s *BaseGQLListener) ExitListValueExpression(ctx *ListValueExpressionContext) {}

// EnterListValueFunction is called when production listValueFunction is entered.
func (s *BaseGQLListener) EnterListValueFunction(ctx *ListValueFunctionContext) {}

// ExitListValueFunction is called when production listValueFunction is exited.
func (s *BaseGQLListener) ExitListValueFunction(ctx *ListValueFunctionContext) {}

// EnterTrimListFunction is called when production trimListFunction is entered.
func (s *BaseGQLListener) EnterTrimListFunction(ctx *TrimListFunctionContext) {}

// ExitTrimListFunction is called when production trimListFunction is exited.
func (s *BaseGQLListener) ExitTrimListFunction(ctx *TrimListFunctionContext) {}

// EnterElementsFunction is called when production elementsFunction is entered.
func (s *BaseGQLListener) EnterElementsFunction(ctx *ElementsFunctionContext) {}

// ExitElementsFunction is called when production elementsFunction is exited.
func (s *BaseGQLListener) ExitElementsFunction(ctx *ElementsFunctionContext) {}

// EnterListValueConstructor is called when production listValueConstructor is entered.
func (s *BaseGQLListener) EnterListValueConstructor(ctx *ListValueConstructorContext) {}

// ExitListValueConstructor is called when production listValueConstructor is exited.
func (s *BaseGQLListener) ExitListValueConstructor(ctx *ListValueConstructorContext) {}

// EnterListValueConstructorByEnumeration is called when production listValueConstructorByEnumeration is entered.
func (s *BaseGQLListener) EnterListValueConstructorByEnumeration(ctx *ListValueConstructorByEnumerationContext) {
}

// ExitListValueConstructorByEnumeration is called when production listValueConstructorByEnumeration is exited.
func (s *BaseGQLListener) ExitListValueConstructorByEnumeration(ctx *ListValueConstructorByEnumerationContext) {
}

// EnterListElementList is called when production listElementList is entered.
func (s *BaseGQLListener) EnterListElementList(ctx *ListElementListContext) {}

// ExitListElementList is called when production listElementList is exited.
func (s *BaseGQLListener) ExitListElementList(ctx *ListElementListContext) {}

// EnterListElement is called when production listElement is entered.
func (s *BaseGQLListener) EnterListElement(ctx *ListElementContext) {}

// ExitListElement is called when production listElement is exited.
func (s *BaseGQLListener) ExitListElement(ctx *ListElementContext) {}

// EnterRecordConstructor is called when production recordConstructor is entered.
func (s *BaseGQLListener) EnterRecordConstructor(ctx *RecordConstructorContext) {}

// ExitRecordConstructor is called when production recordConstructor is exited.
func (s *BaseGQLListener) ExitRecordConstructor(ctx *RecordConstructorContext) {}

// EnterFieldsSpecification is called when production fieldsSpecification is entered.
func (s *BaseGQLListener) EnterFieldsSpecification(ctx *FieldsSpecificationContext) {}

// ExitFieldsSpecification is called when production fieldsSpecification is exited.
func (s *BaseGQLListener) ExitFieldsSpecification(ctx *FieldsSpecificationContext) {}

// EnterFieldList is called when production fieldList is entered.
func (s *BaseGQLListener) EnterFieldList(ctx *FieldListContext) {}

// ExitFieldList is called when production fieldList is exited.
func (s *BaseGQLListener) ExitFieldList(ctx *FieldListContext) {}

// EnterField is called when production field is entered.
func (s *BaseGQLListener) EnterField(ctx *FieldContext) {}

// ExitField is called when production field is exited.
func (s *BaseGQLListener) ExitField(ctx *FieldContext) {}

// EnterTruthValue is called when production truthValue is entered.
func (s *BaseGQLListener) EnterTruthValue(ctx *TruthValueContext) {}

// ExitTruthValue is called when production truthValue is exited.
func (s *BaseGQLListener) ExitTruthValue(ctx *TruthValueContext) {}

// EnterNumericValueExpression is called when production numericValueExpression is entered.
func (s *BaseGQLListener) EnterNumericValueExpression(ctx *NumericValueExpressionContext) {}

// ExitNumericValueExpression is called when production numericValueExpression is exited.
func (s *BaseGQLListener) ExitNumericValueExpression(ctx *NumericValueExpressionContext) {}

// EnterNumericValueFunction is called when production numericValueFunction is entered.
func (s *BaseGQLListener) EnterNumericValueFunction(ctx *NumericValueFunctionContext) {}

// ExitNumericValueFunction is called when production numericValueFunction is exited.
func (s *BaseGQLListener) ExitNumericValueFunction(ctx *NumericValueFunctionContext) {}

// EnterLengthExpression is called when production lengthExpression is entered.
func (s *BaseGQLListener) EnterLengthExpression(ctx *LengthExpressionContext) {}

// ExitLengthExpression is called when production lengthExpression is exited.
func (s *BaseGQLListener) ExitLengthExpression(ctx *LengthExpressionContext) {}

// EnterCardinalityExpression is called when production cardinalityExpression is entered.
func (s *BaseGQLListener) EnterCardinalityExpression(ctx *CardinalityExpressionContext) {}

// ExitCardinalityExpression is called when production cardinalityExpression is exited.
func (s *BaseGQLListener) ExitCardinalityExpression(ctx *CardinalityExpressionContext) {}

// EnterCardinalityExpressionArgument is called when production cardinalityExpressionArgument is entered.
func (s *BaseGQLListener) EnterCardinalityExpressionArgument(ctx *CardinalityExpressionArgumentContext) {
}

// ExitCardinalityExpressionArgument is called when production cardinalityExpressionArgument is exited.
func (s *BaseGQLListener) ExitCardinalityExpressionArgument(ctx *CardinalityExpressionArgumentContext) {
}

// EnterCharLengthExpression is called when production charLengthExpression is entered.
func (s *BaseGQLListener) EnterCharLengthExpression(ctx *CharLengthExpressionContext) {}

// ExitCharLengthExpression is called when production charLengthExpression is exited.
func (s *BaseGQLListener) ExitCharLengthExpression(ctx *CharLengthExpressionContext) {}

// EnterByteLengthExpression is called when production byteLengthExpression is entered.
func (s *BaseGQLListener) EnterByteLengthExpression(ctx *ByteLengthExpressionContext) {}

// ExitByteLengthExpression is called when production byteLengthExpression is exited.
func (s *BaseGQLListener) ExitByteLengthExpression(ctx *ByteLengthExpressionContext) {}

// EnterPathLengthExpression is called when production pathLengthExpression is entered.
func (s *BaseGQLListener) EnterPathLengthExpression(ctx *PathLengthExpressionContext) {}

// ExitPathLengthExpression is called when production pathLengthExpression is exited.
func (s *BaseGQLListener) ExitPathLengthExpression(ctx *PathLengthExpressionContext) {}

// EnterAbsoluteValueExpression is called when production absoluteValueExpression is entered.
func (s *BaseGQLListener) EnterAbsoluteValueExpression(ctx *AbsoluteValueExpressionContext) {}

// ExitAbsoluteValueExpression is called when production absoluteValueExpression is exited.
func (s *BaseGQLListener) ExitAbsoluteValueExpression(ctx *AbsoluteValueExpressionContext) {}

// EnterModulusExpression is called when production modulusExpression is entered.
func (s *BaseGQLListener) EnterModulusExpression(ctx *ModulusExpressionContext) {}

// ExitModulusExpression is called when production modulusExpression is exited.
func (s *BaseGQLListener) ExitModulusExpression(ctx *ModulusExpressionContext) {}

// EnterNumericValueExpressionDividend is called when production numericValueExpressionDividend is entered.
func (s *BaseGQLListener) EnterNumericValueExpressionDividend(ctx *NumericValueExpressionDividendContext) {
}

// ExitNumericValueExpressionDividend is called when production numericValueExpressionDividend is exited.
func (s *BaseGQLListener) ExitNumericValueExpressionDividend(ctx *NumericValueExpressionDividendContext) {
}

// EnterNumericValueExpressionDivisor is called when production numericValueExpressionDivisor is entered.
func (s *BaseGQLListener) EnterNumericValueExpressionDivisor(ctx *NumericValueExpressionDivisorContext) {
}

// ExitNumericValueExpressionDivisor is called when production numericValueExpressionDivisor is exited.
func (s *BaseGQLListener) ExitNumericValueExpressionDivisor(ctx *NumericValueExpressionDivisorContext) {
}

// EnterTrigonometricFunction is called when production trigonometricFunction is entered.
func (s *BaseGQLListener) EnterTrigonometricFunction(ctx *TrigonometricFunctionContext) {}

// ExitTrigonometricFunction is called when production trigonometricFunction is exited.
func (s *BaseGQLListener) ExitTrigonometricFunction(ctx *TrigonometricFunctionContext) {}

// EnterTrigonometricFunctionName is called when production trigonometricFunctionName is entered.
func (s *BaseGQLListener) EnterTrigonometricFunctionName(ctx *TrigonometricFunctionNameContext) {}

// ExitTrigonometricFunctionName is called when production trigonometricFunctionName is exited.
func (s *BaseGQLListener) ExitTrigonometricFunctionName(ctx *TrigonometricFunctionNameContext) {}

// EnterGeneralLogarithmFunction is called when production generalLogarithmFunction is entered.
func (s *BaseGQLListener) EnterGeneralLogarithmFunction(ctx *GeneralLogarithmFunctionContext) {}

// ExitGeneralLogarithmFunction is called when production generalLogarithmFunction is exited.
func (s *BaseGQLListener) ExitGeneralLogarithmFunction(ctx *GeneralLogarithmFunctionContext) {}

// EnterGeneralLogarithmBase is called when production generalLogarithmBase is entered.
func (s *BaseGQLListener) EnterGeneralLogarithmBase(ctx *GeneralLogarithmBaseContext) {}

// ExitGeneralLogarithmBase is called when production generalLogarithmBase is exited.
func (s *BaseGQLListener) ExitGeneralLogarithmBase(ctx *GeneralLogarithmBaseContext) {}

// EnterGeneralLogarithmArgument is called when production generalLogarithmArgument is entered.
func (s *BaseGQLListener) EnterGeneralLogarithmArgument(ctx *GeneralLogarithmArgumentContext) {}

// ExitGeneralLogarithmArgument is called when production generalLogarithmArgument is exited.
func (s *BaseGQLListener) ExitGeneralLogarithmArgument(ctx *GeneralLogarithmArgumentContext) {}

// EnterCommonLogarithm is called when production commonLogarithm is entered.
func (s *BaseGQLListener) EnterCommonLogarithm(ctx *CommonLogarithmContext) {}

// ExitCommonLogarithm is called when production commonLogarithm is exited.
func (s *BaseGQLListener) ExitCommonLogarithm(ctx *CommonLogarithmContext) {}

// EnterNaturalLogarithm is called when production naturalLogarithm is entered.
func (s *BaseGQLListener) EnterNaturalLogarithm(ctx *NaturalLogarithmContext) {}

// ExitNaturalLogarithm is called when production naturalLogarithm is exited.
func (s *BaseGQLListener) ExitNaturalLogarithm(ctx *NaturalLogarithmContext) {}

// EnterExponentialFunction is called when production exponentialFunction is entered.
func (s *BaseGQLListener) EnterExponentialFunction(ctx *ExponentialFunctionContext) {}

// ExitExponentialFunction is called when production exponentialFunction is exited.
func (s *BaseGQLListener) ExitExponentialFunction(ctx *ExponentialFunctionContext) {}

// EnterPowerFunction is called when production powerFunction is entered.
func (s *BaseGQLListener) EnterPowerFunction(ctx *PowerFunctionContext) {}

// ExitPowerFunction is called when production powerFunction is exited.
func (s *BaseGQLListener) ExitPowerFunction(ctx *PowerFunctionContext) {}

// EnterNumericValueExpressionBase is called when production numericValueExpressionBase is entered.
func (s *BaseGQLListener) EnterNumericValueExpressionBase(ctx *NumericValueExpressionBaseContext) {}

// ExitNumericValueExpressionBase is called when production numericValueExpressionBase is exited.
func (s *BaseGQLListener) ExitNumericValueExpressionBase(ctx *NumericValueExpressionBaseContext) {}

// EnterNumericValueExpressionExponent is called when production numericValueExpressionExponent is entered.
func (s *BaseGQLListener) EnterNumericValueExpressionExponent(ctx *NumericValueExpressionExponentContext) {
}

// ExitNumericValueExpressionExponent is called when production numericValueExpressionExponent is exited.
func (s *BaseGQLListener) ExitNumericValueExpressionExponent(ctx *NumericValueExpressionExponentContext) {
}

// EnterSquareRoot is called when production squareRoot is entered.
func (s *BaseGQLListener) EnterSquareRoot(ctx *SquareRootContext) {}

// ExitSquareRoot is called when production squareRoot is exited.
func (s *BaseGQLListener) ExitSquareRoot(ctx *SquareRootContext) {}

// EnterFloorFunction is called when production floorFunction is entered.
func (s *BaseGQLListener) EnterFloorFunction(ctx *FloorFunctionContext) {}

// ExitFloorFunction is called when production floorFunction is exited.
func (s *BaseGQLListener) ExitFloorFunction(ctx *FloorFunctionContext) {}

// EnterCeilingFunction is called when production ceilingFunction is entered.
func (s *BaseGQLListener) EnterCeilingFunction(ctx *CeilingFunctionContext) {}

// ExitCeilingFunction is called when production ceilingFunction is exited.
func (s *BaseGQLListener) ExitCeilingFunction(ctx *CeilingFunctionContext) {}

// EnterCharacterStringValueExpression is called when production characterStringValueExpression is entered.
func (s *BaseGQLListener) EnterCharacterStringValueExpression(ctx *CharacterStringValueExpressionContext) {
}

// ExitCharacterStringValueExpression is called when production characterStringValueExpression is exited.
func (s *BaseGQLListener) ExitCharacterStringValueExpression(ctx *CharacterStringValueExpressionContext) {
}

// EnterByteStringValueExpression is called when production byteStringValueExpression is entered.
func (s *BaseGQLListener) EnterByteStringValueExpression(ctx *ByteStringValueExpressionContext) {}

// ExitByteStringValueExpression is called when production byteStringValueExpression is exited.
func (s *BaseGQLListener) ExitByteStringValueExpression(ctx *ByteStringValueExpressionContext) {}

// EnterTrimOperands is called when production trimOperands is entered.
func (s *BaseGQLListener) EnterTrimOperands(ctx *TrimOperandsContext) {}

// ExitTrimOperands is called when production trimOperands is exited.
func (s *BaseGQLListener) ExitTrimOperands(ctx *TrimOperandsContext) {}

// EnterTrimCharacterOrByteStringSource is called when production trimCharacterOrByteStringSource is entered.
func (s *BaseGQLListener) EnterTrimCharacterOrByteStringSource(ctx *TrimCharacterOrByteStringSourceContext) {
}

// ExitTrimCharacterOrByteStringSource is called when production trimCharacterOrByteStringSource is exited.
func (s *BaseGQLListener) ExitTrimCharacterOrByteStringSource(ctx *TrimCharacterOrByteStringSourceContext) {
}

// EnterTrimSpecification is called when production trimSpecification is entered.
func (s *BaseGQLListener) EnterTrimSpecification(ctx *TrimSpecificationContext) {}

// ExitTrimSpecification is called when production trimSpecification is exited.
func (s *BaseGQLListener) ExitTrimSpecification(ctx *TrimSpecificationContext) {}

// EnterTrimCharacterOrByteString is called when production trimCharacterOrByteString is entered.
func (s *BaseGQLListener) EnterTrimCharacterOrByteString(ctx *TrimCharacterOrByteStringContext) {}

// ExitTrimCharacterOrByteString is called when production trimCharacterOrByteString is exited.
func (s *BaseGQLListener) ExitTrimCharacterOrByteString(ctx *TrimCharacterOrByteStringContext) {}

// EnterNormalForm is called when production normalForm is entered.
func (s *BaseGQLListener) EnterNormalForm(ctx *NormalFormContext) {}

// ExitNormalForm is called when production normalForm is exited.
func (s *BaseGQLListener) ExitNormalForm(ctx *NormalFormContext) {}

// EnterStringLength is called when production stringLength is entered.
func (s *BaseGQLListener) EnterStringLength(ctx *StringLengthContext) {}

// ExitStringLength is called when production stringLength is exited.
func (s *BaseGQLListener) ExitStringLength(ctx *StringLengthContext) {}

// EnterDatetimeValueExpression is called when production datetimeValueExpression is entered.
func (s *BaseGQLListener) EnterDatetimeValueExpression(ctx *DatetimeValueExpressionContext) {}

// ExitDatetimeValueExpression is called when production datetimeValueExpression is exited.
func (s *BaseGQLListener) ExitDatetimeValueExpression(ctx *DatetimeValueExpressionContext) {}

// EnterDatetimeValueFunction is called when production datetimeValueFunction is entered.
func (s *BaseGQLListener) EnterDatetimeValueFunction(ctx *DatetimeValueFunctionContext) {}

// ExitDatetimeValueFunction is called when production datetimeValueFunction is exited.
func (s *BaseGQLListener) ExitDatetimeValueFunction(ctx *DatetimeValueFunctionContext) {}

// EnterDateFunction is called when production dateFunction is entered.
func (s *BaseGQLListener) EnterDateFunction(ctx *DateFunctionContext) {}

// ExitDateFunction is called when production dateFunction is exited.
func (s *BaseGQLListener) ExitDateFunction(ctx *DateFunctionContext) {}

// EnterTimeFunction is called when production timeFunction is entered.
func (s *BaseGQLListener) EnterTimeFunction(ctx *TimeFunctionContext) {}

// ExitTimeFunction is called when production timeFunction is exited.
func (s *BaseGQLListener) ExitTimeFunction(ctx *TimeFunctionContext) {}

// EnterLocaltimeFunction is called when production localtimeFunction is entered.
func (s *BaseGQLListener) EnterLocaltimeFunction(ctx *LocaltimeFunctionContext) {}

// ExitLocaltimeFunction is called when production localtimeFunction is exited.
func (s *BaseGQLListener) ExitLocaltimeFunction(ctx *LocaltimeFunctionContext) {}

// EnterDatetimeFunction is called when production datetimeFunction is entered.
func (s *BaseGQLListener) EnterDatetimeFunction(ctx *DatetimeFunctionContext) {}

// ExitDatetimeFunction is called when production datetimeFunction is exited.
func (s *BaseGQLListener) ExitDatetimeFunction(ctx *DatetimeFunctionContext) {}

// EnterLocaldatetimeFunction is called when production localdatetimeFunction is entered.
func (s *BaseGQLListener) EnterLocaldatetimeFunction(ctx *LocaldatetimeFunctionContext) {}

// ExitLocaldatetimeFunction is called when production localdatetimeFunction is exited.
func (s *BaseGQLListener) ExitLocaldatetimeFunction(ctx *LocaldatetimeFunctionContext) {}

// EnterDateFunctionParameters is called when production dateFunctionParameters is entered.
func (s *BaseGQLListener) EnterDateFunctionParameters(ctx *DateFunctionParametersContext) {}

// ExitDateFunctionParameters is called when production dateFunctionParameters is exited.
func (s *BaseGQLListener) ExitDateFunctionParameters(ctx *DateFunctionParametersContext) {}

// EnterTimeFunctionParameters is called when production timeFunctionParameters is entered.
func (s *BaseGQLListener) EnterTimeFunctionParameters(ctx *TimeFunctionParametersContext) {}

// ExitTimeFunctionParameters is called when production timeFunctionParameters is exited.
func (s *BaseGQLListener) ExitTimeFunctionParameters(ctx *TimeFunctionParametersContext) {}

// EnterDatetimeFunctionParameters is called when production datetimeFunctionParameters is entered.
func (s *BaseGQLListener) EnterDatetimeFunctionParameters(ctx *DatetimeFunctionParametersContext) {}

// ExitDatetimeFunctionParameters is called when production datetimeFunctionParameters is exited.
func (s *BaseGQLListener) ExitDatetimeFunctionParameters(ctx *DatetimeFunctionParametersContext) {}

// EnterDurationValueExpression is called when production durationValueExpression is entered.
func (s *BaseGQLListener) EnterDurationValueExpression(ctx *DurationValueExpressionContext) {}

// ExitDurationValueExpression is called when production durationValueExpression is exited.
func (s *BaseGQLListener) ExitDurationValueExpression(ctx *DurationValueExpressionContext) {}

// EnterDatetimeSubtraction is called when production datetimeSubtraction is entered.
func (s *BaseGQLListener) EnterDatetimeSubtraction(ctx *DatetimeSubtractionContext) {}

// ExitDatetimeSubtraction is called when production datetimeSubtraction is exited.
func (s *BaseGQLListener) ExitDatetimeSubtraction(ctx *DatetimeSubtractionContext) {}

// EnterDatetimeSubtractionParameters is called when production datetimeSubtractionParameters is entered.
func (s *BaseGQLListener) EnterDatetimeSubtractionParameters(ctx *DatetimeSubtractionParametersContext) {
}

// ExitDatetimeSubtractionParameters is called when production datetimeSubtractionParameters is exited.
func (s *BaseGQLListener) ExitDatetimeSubtractionParameters(ctx *DatetimeSubtractionParametersContext) {
}

// EnterDatetimeValueExpression1 is called when production datetimeValueExpression1 is entered.
func (s *BaseGQLListener) EnterDatetimeValueExpression1(ctx *DatetimeValueExpression1Context) {}

// ExitDatetimeValueExpression1 is called when production datetimeValueExpression1 is exited.
func (s *BaseGQLListener) ExitDatetimeValueExpression1(ctx *DatetimeValueExpression1Context) {}

// EnterDatetimeValueExpression2 is called when production datetimeValueExpression2 is entered.
func (s *BaseGQLListener) EnterDatetimeValueExpression2(ctx *DatetimeValueExpression2Context) {}

// ExitDatetimeValueExpression2 is called when production datetimeValueExpression2 is exited.
func (s *BaseGQLListener) ExitDatetimeValueExpression2(ctx *DatetimeValueExpression2Context) {}

// EnterDurationValueFunction is called when production durationValueFunction is entered.
func (s *BaseGQLListener) EnterDurationValueFunction(ctx *DurationValueFunctionContext) {}

// ExitDurationValueFunction is called when production durationValueFunction is exited.
func (s *BaseGQLListener) ExitDurationValueFunction(ctx *DurationValueFunctionContext) {}

// EnterDurationFunction is called when production durationFunction is entered.
func (s *BaseGQLListener) EnterDurationFunction(ctx *DurationFunctionContext) {}

// ExitDurationFunction is called when production durationFunction is exited.
func (s *BaseGQLListener) ExitDurationFunction(ctx *DurationFunctionContext) {}

// EnterDurationFunctionParameters is called when production durationFunctionParameters is entered.
func (s *BaseGQLListener) EnterDurationFunctionParameters(ctx *DurationFunctionParametersContext) {}

// ExitDurationFunctionParameters is called when production durationFunctionParameters is exited.
func (s *BaseGQLListener) ExitDurationFunctionParameters(ctx *DurationFunctionParametersContext) {}

// EnterObjectName is called when production objectName is entered.
func (s *BaseGQLListener) EnterObjectName(ctx *ObjectNameContext) {}

// ExitObjectName is called when production objectName is exited.
func (s *BaseGQLListener) ExitObjectName(ctx *ObjectNameContext) {}

// EnterObjectNameOrBindingVariable is called when production objectNameOrBindingVariable is entered.
func (s *BaseGQLListener) EnterObjectNameOrBindingVariable(ctx *ObjectNameOrBindingVariableContext) {}

// ExitObjectNameOrBindingVariable is called when production objectNameOrBindingVariable is exited.
func (s *BaseGQLListener) ExitObjectNameOrBindingVariable(ctx *ObjectNameOrBindingVariableContext) {}

// EnterDirectoryName is called when production directoryName is entered.
func (s *BaseGQLListener) EnterDirectoryName(ctx *DirectoryNameContext) {}

// ExitDirectoryName is called when production directoryName is exited.
func (s *BaseGQLListener) ExitDirectoryName(ctx *DirectoryNameContext) {}

// EnterSchemaName is called when production schemaName is entered.
func (s *BaseGQLListener) EnterSchemaName(ctx *SchemaNameContext) {}

// ExitSchemaName is called when production schemaName is exited.
func (s *BaseGQLListener) ExitSchemaName(ctx *SchemaNameContext) {}

// EnterGraphName is called when production graphName is entered.
func (s *BaseGQLListener) EnterGraphName(ctx *GraphNameContext) {}

// ExitGraphName is called when production graphName is exited.
func (s *BaseGQLListener) ExitGraphName(ctx *GraphNameContext) {}

// EnterDelimitedGraphName is called when production delimitedGraphName is entered.
func (s *BaseGQLListener) EnterDelimitedGraphName(ctx *DelimitedGraphNameContext) {}

// ExitDelimitedGraphName is called when production delimitedGraphName is exited.
func (s *BaseGQLListener) ExitDelimitedGraphName(ctx *DelimitedGraphNameContext) {}

// EnterGraphTypeName is called when production graphTypeName is entered.
func (s *BaseGQLListener) EnterGraphTypeName(ctx *GraphTypeNameContext) {}

// ExitGraphTypeName is called when production graphTypeName is exited.
func (s *BaseGQLListener) ExitGraphTypeName(ctx *GraphTypeNameContext) {}

// EnterNodeTypeName is called when production nodeTypeName is entered.
func (s *BaseGQLListener) EnterNodeTypeName(ctx *NodeTypeNameContext) {}

// ExitNodeTypeName is called when production nodeTypeName is exited.
func (s *BaseGQLListener) ExitNodeTypeName(ctx *NodeTypeNameContext) {}

// EnterEdgeTypeName is called when production edgeTypeName is entered.
func (s *BaseGQLListener) EnterEdgeTypeName(ctx *EdgeTypeNameContext) {}

// ExitEdgeTypeName is called when production edgeTypeName is exited.
func (s *BaseGQLListener) ExitEdgeTypeName(ctx *EdgeTypeNameContext) {}

// EnterBindingTableName is called when production bindingTableName is entered.
func (s *BaseGQLListener) EnterBindingTableName(ctx *BindingTableNameContext) {}

// ExitBindingTableName is called when production bindingTableName is exited.
func (s *BaseGQLListener) ExitBindingTableName(ctx *BindingTableNameContext) {}

// EnterDelimitedBindingTableName is called when production delimitedBindingTableName is entered.
func (s *BaseGQLListener) EnterDelimitedBindingTableName(ctx *DelimitedBindingTableNameContext) {}

// ExitDelimitedBindingTableName is called when production delimitedBindingTableName is exited.
func (s *BaseGQLListener) ExitDelimitedBindingTableName(ctx *DelimitedBindingTableNameContext) {}

// EnterProcedureName is called when production procedureName is entered.
func (s *BaseGQLListener) EnterProcedureName(ctx *ProcedureNameContext) {}

// ExitProcedureName is called when production procedureName is exited.
func (s *BaseGQLListener) ExitProcedureName(ctx *ProcedureNameContext) {}

// EnterLabelName is called when production labelName is entered.
func (s *BaseGQLListener) EnterLabelName(ctx *LabelNameContext) {}

// ExitLabelName is called when production labelName is exited.
func (s *BaseGQLListener) ExitLabelName(ctx *LabelNameContext) {}

// EnterPropertyName is called when production propertyName is entered.
func (s *BaseGQLListener) EnterPropertyName(ctx *PropertyNameContext) {}

// ExitPropertyName is called when production propertyName is exited.
func (s *BaseGQLListener) ExitPropertyName(ctx *PropertyNameContext) {}

// EnterFieldName is called when production fieldName is entered.
func (s *BaseGQLListener) EnterFieldName(ctx *FieldNameContext) {}

// ExitFieldName is called when production fieldName is exited.
func (s *BaseGQLListener) ExitFieldName(ctx *FieldNameContext) {}

// EnterElementVariable is called when production elementVariable is entered.
func (s *BaseGQLListener) EnterElementVariable(ctx *ElementVariableContext) {}

// ExitElementVariable is called when production elementVariable is exited.
func (s *BaseGQLListener) ExitElementVariable(ctx *ElementVariableContext) {}

// EnterPathVariable is called when production pathVariable is entered.
func (s *BaseGQLListener) EnterPathVariable(ctx *PathVariableContext) {}

// ExitPathVariable is called when production pathVariable is exited.
func (s *BaseGQLListener) ExitPathVariable(ctx *PathVariableContext) {}

// EnterSubpathVariable is called when production subpathVariable is entered.
func (s *BaseGQLListener) EnterSubpathVariable(ctx *SubpathVariableContext) {}

// ExitSubpathVariable is called when production subpathVariable is exited.
func (s *BaseGQLListener) ExitSubpathVariable(ctx *SubpathVariableContext) {}

// EnterBindingVariable is called when production bindingVariable is entered.
func (s *BaseGQLListener) EnterBindingVariable(ctx *BindingVariableContext) {}

// ExitBindingVariable is called when production bindingVariable is exited.
func (s *BaseGQLListener) ExitBindingVariable(ctx *BindingVariableContext) {}

// EnterUnsignedLiteral is called when production unsignedLiteral is entered.
func (s *BaseGQLListener) EnterUnsignedLiteral(ctx *UnsignedLiteralContext) {}

// ExitUnsignedLiteral is called when production unsignedLiteral is exited.
func (s *BaseGQLListener) ExitUnsignedLiteral(ctx *UnsignedLiteralContext) {}

// EnterGeneralLiteral is called when production generalLiteral is entered.
func (s *BaseGQLListener) EnterGeneralLiteral(ctx *GeneralLiteralContext) {}

// ExitGeneralLiteral is called when production generalLiteral is exited.
func (s *BaseGQLListener) ExitGeneralLiteral(ctx *GeneralLiteralContext) {}

// EnterTemporalLiteral is called when production temporalLiteral is entered.
func (s *BaseGQLListener) EnterTemporalLiteral(ctx *TemporalLiteralContext) {}

// ExitTemporalLiteral is called when production temporalLiteral is exited.
func (s *BaseGQLListener) ExitTemporalLiteral(ctx *TemporalLiteralContext) {}

// EnterDateLiteral is called when production dateLiteral is entered.
func (s *BaseGQLListener) EnterDateLiteral(ctx *DateLiteralContext) {}

// ExitDateLiteral is called when production dateLiteral is exited.
func (s *BaseGQLListener) ExitDateLiteral(ctx *DateLiteralContext) {}

// EnterTimeLiteral is called when production timeLiteral is entered.
func (s *BaseGQLListener) EnterTimeLiteral(ctx *TimeLiteralContext) {}

// ExitTimeLiteral is called when production timeLiteral is exited.
func (s *BaseGQLListener) ExitTimeLiteral(ctx *TimeLiteralContext) {}

// EnterDatetimeLiteral is called when production datetimeLiteral is entered.
func (s *BaseGQLListener) EnterDatetimeLiteral(ctx *DatetimeLiteralContext) {}

// ExitDatetimeLiteral is called when production datetimeLiteral is exited.
func (s *BaseGQLListener) ExitDatetimeLiteral(ctx *DatetimeLiteralContext) {}

// EnterListLiteral is called when production listLiteral is entered.
func (s *BaseGQLListener) EnterListLiteral(ctx *ListLiteralContext) {}

// ExitListLiteral is called when production listLiteral is exited.
func (s *BaseGQLListener) ExitListLiteral(ctx *ListLiteralContext) {}

// EnterRecordLiteral is called when production recordLiteral is entered.
func (s *BaseGQLListener) EnterRecordLiteral(ctx *RecordLiteralContext) {}

// ExitRecordLiteral is called when production recordLiteral is exited.
func (s *BaseGQLListener) ExitRecordLiteral(ctx *RecordLiteralContext) {}

// EnterIdentifier is called when production identifier is entered.
func (s *BaseGQLListener) EnterIdentifier(ctx *IdentifierContext) {}

// ExitIdentifier is called when production identifier is exited.
func (s *BaseGQLListener) ExitIdentifier(ctx *IdentifierContext) {}

// EnterRegularIdentifier is called when production regularIdentifier is entered.
func (s *BaseGQLListener) EnterRegularIdentifier(ctx *RegularIdentifierContext) {}

// ExitRegularIdentifier is called when production regularIdentifier is exited.
func (s *BaseGQLListener) ExitRegularIdentifier(ctx *RegularIdentifierContext) {}

// EnterTimeZoneString is called when production timeZoneString is entered.
func (s *BaseGQLListener) EnterTimeZoneString(ctx *TimeZoneStringContext) {}

// ExitTimeZoneString is called when production timeZoneString is exited.
func (s *BaseGQLListener) ExitTimeZoneString(ctx *TimeZoneStringContext) {}

// EnterCharacterStringLiteral is called when production characterStringLiteral is entered.
func (s *BaseGQLListener) EnterCharacterStringLiteral(ctx *CharacterStringLiteralContext) {}

// ExitCharacterStringLiteral is called when production characterStringLiteral is exited.
func (s *BaseGQLListener) ExitCharacterStringLiteral(ctx *CharacterStringLiteralContext) {}

// EnterUnsignedNumericLiteral is called when production unsignedNumericLiteral is entered.
func (s *BaseGQLListener) EnterUnsignedNumericLiteral(ctx *UnsignedNumericLiteralContext) {}

// ExitUnsignedNumericLiteral is called when production unsignedNumericLiteral is exited.
func (s *BaseGQLListener) ExitUnsignedNumericLiteral(ctx *UnsignedNumericLiteralContext) {}

// EnterExactNumericLiteral is called when production exactNumericLiteral is entered.
func (s *BaseGQLListener) EnterExactNumericLiteral(ctx *ExactNumericLiteralContext) {}

// ExitExactNumericLiteral is called when production exactNumericLiteral is exited.
func (s *BaseGQLListener) ExitExactNumericLiteral(ctx *ExactNumericLiteralContext) {}

// EnterApproximateNumericLiteral is called when production approximateNumericLiteral is entered.
func (s *BaseGQLListener) EnterApproximateNumericLiteral(ctx *ApproximateNumericLiteralContext) {}

// ExitApproximateNumericLiteral is called when production approximateNumericLiteral is exited.
func (s *BaseGQLListener) ExitApproximateNumericLiteral(ctx *ApproximateNumericLiteralContext) {}

// EnterUnsignedInteger is called when production unsignedInteger is entered.
func (s *BaseGQLListener) EnterUnsignedInteger(ctx *UnsignedIntegerContext) {}

// ExitUnsignedInteger is called when production unsignedInteger is exited.
func (s *BaseGQLListener) ExitUnsignedInteger(ctx *UnsignedIntegerContext) {}

// EnterUnsignedDecimalInteger is called when production unsignedDecimalInteger is entered.
func (s *BaseGQLListener) EnterUnsignedDecimalInteger(ctx *UnsignedDecimalIntegerContext) {}

// ExitUnsignedDecimalInteger is called when production unsignedDecimalInteger is exited.
func (s *BaseGQLListener) ExitUnsignedDecimalInteger(ctx *UnsignedDecimalIntegerContext) {}

// EnterNullLiteral is called when production nullLiteral is entered.
func (s *BaseGQLListener) EnterNullLiteral(ctx *NullLiteralContext) {}

// ExitNullLiteral is called when production nullLiteral is exited.
func (s *BaseGQLListener) ExitNullLiteral(ctx *NullLiteralContext) {}

// EnterDateString is called when production dateString is entered.
func (s *BaseGQLListener) EnterDateString(ctx *DateStringContext) {}

// ExitDateString is called when production dateString is exited.
func (s *BaseGQLListener) ExitDateString(ctx *DateStringContext) {}

// EnterTimeString is called when production timeString is entered.
func (s *BaseGQLListener) EnterTimeString(ctx *TimeStringContext) {}

// ExitTimeString is called when production timeString is exited.
func (s *BaseGQLListener) ExitTimeString(ctx *TimeStringContext) {}

// EnterDatetimeString is called when production datetimeString is entered.
func (s *BaseGQLListener) EnterDatetimeString(ctx *DatetimeStringContext) {}

// ExitDatetimeString is called when production datetimeString is exited.
func (s *BaseGQLListener) ExitDatetimeString(ctx *DatetimeStringContext) {}

// EnterDurationLiteral is called when production durationLiteral is entered.
func (s *BaseGQLListener) EnterDurationLiteral(ctx *DurationLiteralContext) {}

// ExitDurationLiteral is called when production durationLiteral is exited.
func (s *BaseGQLListener) ExitDurationLiteral(ctx *DurationLiteralContext) {}

// EnterDurationString is called when production durationString is entered.
func (s *BaseGQLListener) EnterDurationString(ctx *DurationStringContext) {}

// ExitDurationString is called when production durationString is exited.
func (s *BaseGQLListener) ExitDurationString(ctx *DurationStringContext) {}

// EnterNodeSynonym is called when production nodeSynonym is entered.
func (s *BaseGQLListener) EnterNodeSynonym(ctx *NodeSynonymContext) {}

// ExitNodeSynonym is called when production nodeSynonym is exited.
func (s *BaseGQLListener) ExitNodeSynonym(ctx *NodeSynonymContext) {}

// EnterEdgesSynonym is called when production edgesSynonym is entered.
func (s *BaseGQLListener) EnterEdgesSynonym(ctx *EdgesSynonymContext) {}

// ExitEdgesSynonym is called when production edgesSynonym is exited.
func (s *BaseGQLListener) ExitEdgesSynonym(ctx *EdgesSynonymContext) {}

// EnterEdgeSynonym is called when production edgeSynonym is entered.
func (s *BaseGQLListener) EnterEdgeSynonym(ctx *EdgeSynonymContext) {}

// ExitEdgeSynonym is called when production edgeSynonym is exited.
func (s *BaseGQLListener) ExitEdgeSynonym(ctx *EdgeSynonymContext) {}

// EnterNonReservedWords is called when production nonReservedWords is entered.
func (s *BaseGQLListener) EnterNonReservedWords(ctx *NonReservedWordsContext) {}

// ExitNonReservedWords is called when production nonReservedWords is exited.
func (s *BaseGQLListener) ExitNonReservedWords(ctx *NonReservedWordsContext) {}
