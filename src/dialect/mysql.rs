// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#[cfg(not(feature = "std"))]
use alloc::boxed::Box;

use crate::{
    ast::{BinaryOperator, Expr, Ident, Statement},
    dialect::Dialect,
    keywords::Keyword,
    parser::{Parser, ParserError},
};

/// A [`Dialect`] for [MySQL](https://www.mysql.com/)
#[derive(Debug)]
pub struct MySqlDialect {}

impl Dialect for MySqlDialect {
    fn is_identifier_start(&self, ch: char) -> bool {
        // See https://dev.mysql.com/doc/refman/8.0/en/identifiers.html.
        // Identifiers which begin with a digit are recognized while tokenizing numbers,
        // so they can be distinguished from exponent numeric literals.
        ch.is_alphabetic()
            || ch == '_'
            || ch == '$'
            || ch == '@'
            || ('\u{0080}'..='\u{ffff}').contains(&ch)
    }

    fn is_identifier_part(&self, ch: char) -> bool {
        self.is_identifier_start(ch) || ch.is_ascii_digit()
    }

    fn is_delimited_identifier_start(&self, ch: char) -> bool {
        ch == '`'
    }

    fn identifier_quote_style(&self, _identifier: &str) -> Option<char> {
        Some('`')
    }

    // See https://dev.mysql.com/doc/refman/8.0/en/string-literals.html#character-escape-sequences
    fn supports_string_literal_backslash_escape(&self) -> bool {
        true
    }

    fn supports_numeric_prefix(&self) -> bool {
        true
    }

    fn parse_infix(
        &self,
        parser: &mut crate::parser::Parser,
        expr: &mut crate::ast::Expr,
        _precedence: u8,
    ) -> Option<Result<(), ParserError>> {
        // Parse DIV as an operator
        if parser.parse_keyword(Keyword::DIV) {
            let left = std::mem::replace(
                expr,
                Expr::Identifier(Ident {
                    value: String::new(),
                    quote_style: None,
                }),
            );
            *expr = Expr::BinaryOp {
                left: Box::new(left),
                op: BinaryOperator::MyIntegerDivide,
                right: Box::new(parser.parse_expr().unwrap()),
            };
            Some(Ok(()))
        } else {
            None
        }
    }

    fn parse_statement(&self, parser: &mut Parser) -> Option<Result<Statement, ParserError>> {
        if parser.parse_keywords(&[Keyword::LOCK, Keyword::TABLES]) {
            Some(parse_lock_tables(parser))
        } else if parser.parse_keywords(&[Keyword::UNLOCK, Keyword::TABLES]) {
            Some(parse_unlock_tables(parser))
        } else {
            None
        }
    }

    fn require_interval_qualifier(&self) -> bool {
        true
    }
}

/// `LOCK TABLES`
/// <https://dev.mysql.com/doc/refman/8.0/en/lock-tables.html>
fn parse_lock_tables(_parser: &mut Parser) -> Result<Statement, ParserError> {
    #[cfg(not(feature = "full-ast"))]
    {
        Err(ParserError::unsupported_statement("lock_tables"))
    }
    #[cfg(feature = "full-ast")]
    {
        let tables = _parser.parse_comma_separated(parse_lock_table)?;
        Ok(Statement::LockTables { tables })
    }
}

// tbl_name [[AS] alias] lock_type
#[cfg(feature = "full-ast")]
fn parse_lock_table(parser: &mut Parser) -> Result<crate::ast::LockTable, ParserError> {
    let table = parser.parse_identifier(false)?;
    let alias =
        parser.parse_optional_alias(&[Keyword::READ, Keyword::WRITE, Keyword::LOW_PRIORITY])?;
    let lock_type = parse_lock_tables_type(parser)?;

    Ok(crate::ast::LockTable {
        table,
        alias,
        lock_type,
    })
}

// READ [LOCAL] | [LOW_PRIORITY] WRITE
#[cfg(feature = "full-ast")]
fn parse_lock_tables_type(parser: &mut Parser) -> Result<crate::ast::LockTableType, ParserError> {
    if parser.parse_keyword(Keyword::READ) {
        if parser.parse_keyword(Keyword::LOCAL) {
            Ok(crate::ast::LockTableType::Read { local: true })
        } else {
            Ok(crate::ast::LockTableType::Read { local: false })
        }
    } else if parser.parse_keyword(Keyword::WRITE) {
        Ok(crate::ast::LockTableType::Write {
            low_priority: false,
        })
    } else if parser.parse_keywords(&[Keyword::LOW_PRIORITY, Keyword::WRITE]) {
        Ok(crate::ast::LockTableType::Write { low_priority: true })
    } else {
        parser.expected("an lock type in LOCK TABLES", parser.peek_token())
    }
}

/// UNLOCK TABLES
/// <https://dev.mysql.com/doc/refman/8.0/en/lock-tables.html>
fn parse_unlock_tables(_parser: &mut Parser) -> Result<Statement, ParserError> {
    #[cfg(not(feature = "full-ast"))]
    {
        Err(ParserError::unsupported_statement("unlock_tables"))
    }
    #[cfg(feature = "full-ast")]
    {
        Ok(Statement::UnlockTables)
    }
}
