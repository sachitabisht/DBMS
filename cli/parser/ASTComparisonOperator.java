/* Generated By:JJTree: Do not edit this line. ASTComparisonOperator.java Version 7.0 */
/* JavaCCOptions:MULTI=true,NODE_USES_PARSER=false,VISITOR=true,TRACK_TOKENS=false,NODE_PREFIX=AST,NODE_EXTENDS=,NODE_FACTORY=,SUPPORT_CLASS_VISIBILITY_PUBLIC=true */
package edu.berkeley.cs186.database.cli.parser;

public
class ASTComparisonOperator extends SimpleNode {
  public ASTComparisonOperator(int id) {
    super(id);
  }

  public ASTComparisonOperator(RookieParser p, int id) {
    super(p, id);
  }

  /** Accept the visitor. **/
  public void jjtAccept(RookieParserVisitor visitor, Object data) {
    visitor.visit(this, data);
  }
}
/* JavaCC - OriginalChecksum=4f533bbe1c657509d3a4d36536312bae (do not edit this line) */
