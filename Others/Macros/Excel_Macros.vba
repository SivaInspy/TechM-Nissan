Sub PaintFormatter()
'
' PaintFormatter Macro
' Copy and paste format of the cell
'
' Keyboard Shortcut: Ctrl+Shift+V
'
    If Application.ClipboardFormats(1) = 0 Then
    Selection.PasteSpecial Paste:=xlPasteFormats, Operation:=xlNone, _
        SkipBlanks:=False, Transpose:=False
    Application.CutCopyMode = False
    Else
    MsgBox "Your clipboard is empty - retry copying your data."
    End
    End If
End Sub
Sub NoFill()
'
' NoFill Macro
' No fill in the background
'
' Keyboard Shortcut: Ctrl+Shift+S
'
    With Selection.Interior
        .Pattern = xlNone
        .TintAndShade = 0
        .PatternTintAndShade = 0
    End With
End Sub
Sub LightsUpDateColumn()
'
' LightsUpDateColumn Macro
'
' Keyboard Shortcut: Ctrl+Shift+B
'
    Dim x As Long
    For x = ActiveSheet.Range("C" & ActiveSheet.Rows.Count).End(xlUp).Row To 2 Step -1
    If CDate(ActiveSheet.Range("C" & x).Value) < Date - 1 Then
    ActiveSheet.Range("B" & x & ":" & "C" & x).Interior.Color = vbYellow
    ActiveSheet.Range("B" & x & ":" & "C" & x).Font.Color = vbRed
    End If
    Next x
End Sub
Sub Merge_Same_Cells()
'
' Merge_Same_Cells Macro
'
' Keyboard Shortcut: Ctrl+Shift+M
'
Application.DisplayAlerts = False

Dim rng As Range

MergeCells:

For Each rng In Selection

If rng.Value = rng.Offset(1, 0).Value And rng.Value <> "" Then
Range(rng, rng.Offset(1, 0)).Merge
Range(rng, rng.Offset(1, 0)).HorizontalAlignment = xlCenter
Range(rng, rng.Offset(1, 0)).VerticalAlignment = xlCenter
GoTo MergeCells
End If
Next
End Sub
Sub Unmerge_Fill()
'
' Unmerge_Fill Macro
'
' Keyboard Shortcut: Ctrl+Shift+U
'
'Dim cell As Range, joinedCells As Range
'
'For Each cell In Selection
'    If cell.MergeCells Then
'        Set joinedCells = cell.MergeArea
'        cell.MergeCells = False
'        joinedCells.Value = cell.Value
'    End If
'Next
Set myRange = Application.Selection
Set myRange = Application.InputBox("Select one Range which contain merged Cells", "UnmergeMulCells", myRange.Address, Type:=8)

For Each myCell In myRange
    If myCell.MergeCells Then
        With myCell.MergeArea
            .UnMerge
            .Formula = myCell.Formula
        End With
    End If
Next
End Sub
Sub MergeAndFillMultipleLinesInToOne()
'
' MergeAndFillMultipleLinesInToOne Macro
'
' Keyboard Shortcut: Ctrl+Shift+C
'
Set myRange = Application.Selection

    With Selection.Resize(1, 2)
        .WrapText = True
        .Merge
        .RowHeight = 45
    End With
End Sub
Sub convertMultipleRowsToOneRow()
    Set myRange = Application.InputBox("select one range that you want to convert:", "", Type:=8)
    Set dRang = Application.InputBox("Select one Cell to place data:", "", Type:=8)
    RowNum = myRange.Rows.Count
    colNum = myRange.Columns.Count
    For i = 1 To RowNum
        myRange.Rows(i).Copy dRang
        Set dRang = dRang.Offset(0, colNum + 0)
    Next
End Sub
Sub MultipleRowsToOneCell()
'
' MultipleRowsToOneCell Macro
'
' Keyboard Shortcut: Ctrl+Shift+E

' Tip: Pressing one time Ctrl+Shift+J in replace box is Alt+Enter

    Dim myRange, dRang As Variant

    Set myRange = Application.InputBox("Select one range that you want to convert:", "", Type:=8)
    Set dRang = Application.InputBox("Select one Cell to place data:", "", Type:=8)
    MsgBox dRang(0).Address
    dRang.Formula = "=CONCATENATE(TRANSPOSE(myRange.address),CHAR(10))"
    MsgBox myRange.Address
    dRang.Select
'    Application.Wait (Now() + TimeValue("00:00:01"))
''    MsgBox ActiveCell.Value
'    SendKeys "{F2}"
'    SendKeys "{F9}"
'    SendKeys "^{END}"
'    SendKeys "{BS}"
'    SendKeys "{BS}"
'    SendKeys "{BS}"
'    SendKeys "^{HOME}"
'    SendKeys "{DELETE}"
'    SendKeys "{DELETE}"
'    SendKeys "{DELETE}"
'    SendKeys "{ENTER}"
'    SendKeys "{UP}"
End Sub
Sub ReplaceQuotesInCells()
'
' ReplaceQuotesInCells Macro
'

'
    Cells.Replace What:=""",""", Replacement:="", LookAt:=xlPart, _
        SearchOrder:=xlByRows, MatchCase:=False, SearchFormat:=False, _
        ReplaceFormat:=False
    With Selection
        .HorizontalAlignment = xlGeneral
        .VerticalAlignment = xlBottom
        .WrapText = True
        .Orientation = 0
        .AddIndent = False
        .IndentLevel = 0
        .ShrinkToFit = False
        .ReadingOrder = xlContext
        .MergeCells = False
        .Columns.AutoFit
        .Rows.AutoFit
    End With
    Selection.EntireColumn.AutoFit
End Sub
Sub Macro2()
'
' Macro2 Macro
'

'
    Range(Selection, Selection.End(xlToRight)).Select
    Range("B1").Select
    Range(Selection, Selection.End(xlDown)).Select
    Range("B1").Select
End Sub
Sub convertMultipleColumnsToOneColumn()
    Set myRange = Application.InputBox("select one range that you want to convert:", "", Type:=8)
    Set dRang = Application.InputBox("Select one Cell to place data:", "", Type:=8)
    RowNum = myRange.Rows.Count
    colNum = myRange.Columns.Count
    For i = 1 To colNum
        myRange.Columns(i).Copy dRang
        Set dRang = dRang.Offset(0, RowNum + 0)
    Next
End Sub
Sub SpaceDelimetedTextToTable()
'
' SpaceDelimetedTextToTable Macro
'
' Keyboard Shortcut: Ctrl+Shift+R
'
    Selection.TextToColumns Destination:=Range("A1"), DataType:=xlDelimited, _
        TextQualifier:=xlDoubleQuote, ConsecutiveDelimiter:=True, Tab:=False, _
        Semicolon:=False, Comma:=False, Space:=True, Other:=False, FieldInfo _
        :=Array(Array(1, 1), Array(2, 1), Array(3, 1), Array(4, 1)), TrailingMinusNumbers:= _
        True
End Sub
