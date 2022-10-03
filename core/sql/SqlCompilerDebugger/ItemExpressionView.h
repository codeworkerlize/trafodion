// @@@ START COPYRIGHT @@@
//
// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
//
// @@@ END COPYRIGHT @@@
#ifndef ITEMEXPRESSIONVIEW_H
#define ITEMEXPRESSIONVIEW_H

#include <QtGlobal>
#if (QT_VERSION >= QT_VERSION_CHECK(5, 0, 0))
#include <QtWidgets>
#else
#include <QtGui>
#endif

#include "CommonSqlCmpDbg.h"
namespace Ui
{
  class ItemExpressionView;
}

class ItemExpressionView:public QWidget
{
Q_OBJECT
public:
    explicit ItemExpressionView (QWidget * parent = 0);
   ~ItemExpressionView ();
  void setNewExpr (void *expr);
  void UpdateView (void);
  void Free (void);
  void DisplayItemExprRoot (void *tree);
  void DisplayItemExprChild (void *tree, QTreeWidgetItem * parentTreeItem);
  void FillItemExprDetails (const ExprNode * en, QTreeWidgetItem * parentTreeItem);
private:
  Ui::ItemExpressionView * ui;
  void *m_expr;
private slots:
  void on_treeWidget_itemClicked(QTreeWidgetItem * item, int column);
};

#endif // ITEMEXPRESSIONVIEW_H