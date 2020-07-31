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
Sub NissanJsonToTable()
'
' JsonToTable Macro
'

'
    Range("A1").Select
    Cells.Replace What:="[{""type"":""TABLE"",""data"":""", Replacement:="", _
        LookAt:=xlPart, SearchOrder:=xlByRows, MatchCase:=False, SearchFormat:= _
        False, ReplaceFormat:=False
    Selection.Replace What:="{""type"":""TABLE"",""data"":""", Replacement:="" _
        , LookAt:=xlPart, SearchOrder:=xlByRows, MatchCase:=False, SearchFormat _
        :=False, ReplaceFormat:=False
    Selection.Replace What:="]", Replacement:="", LookAt:=xlPart, _
        SearchOrder:=xlByRows, MatchCase:=False, SearchFormat:=False, _
        ReplaceFormat:=False
    Selection.Replace What:="\n""},", Replacement:=":", LookAt:=xlPart, _
        SearchOrder:=xlByRows, MatchCase:=False, SearchFormat:=False, _
        ReplaceFormat:=False
    Selection.Replace What:="\t", Replacement:=",", LookAt:=xlPart, _
        SearchOrder:=xlByRows, MatchCase:=False, SearchFormat:=False, _
        ReplaceFormat:=False
    Selection.Replace What:="__date", Replacement:="", LookAt:=xlPart, _
        SearchOrder:=xlByRows, MatchCase:=False, SearchFormat:=False, _
        ReplaceFormat:=False
    Selection.Replace What:="__", Replacement:=".", LookAt:=xlPart, _
        SearchOrder:=xlByRows, MatchCase:=False, SearchFormat:=False, _
        ReplaceFormat:=False
    Selection.Replace What:="\n", Replacement:=".", LookAt:=xlPart, _
        SearchOrder:=xlByRows, MatchCase:=False, SearchFormat:=False, _
        ReplaceFormat:=False
    Selection.Replace What:=".""}", Replacement:="", LookAt:=xlPart, _
        SearchOrder:=xlByRows, MatchCase:=False, SearchFormat:=False, _
        ReplaceFormat:=False
    Selection.Replace What:="recordscount.", Replacement:="", LookAt:=xlPart, _
        SearchOrder:=xlByRows, MatchCase:=False, SearchFormat:=False, _
        ReplaceFormat:=False

    Range("A1").Select
    Selection.TextToColumns Destination:=Range("A1"), DataType:=xlDelimited, _
        TextQualifier:=xlDoubleQuote, ConsecutiveDelimiter:=True, Tab:=False, _
        Semicolon:=False, Comma:=False, Space:=False, Other:=True, OtherChar _
        :=":", FieldInfo:=Array(Array(1, 1), Array(2, 1), Array(3, 1), Array(4, 1), Array(5, _
        1), Array(6, 1), Array(7, 1), Array(8, 1), Array(9, 1), Array(10, 1), Array(11, 1), Array(12 _
        , 1), Array(13, 1), Array(14, 1), Array(15, 1), Array(16, 1), Array(17, 1), Array(18, 1), _
        Array(19, 1), Array(20, 1), Array(21, 1), Array(22, 1), Array(23, 1), Array(24, 1), Array( _
        25, 1), Array(26, 1), Array(27, 1), Array(28, 1), Array(29, 1), Array(30, 1), Array(31, 1), _
        Array(32, 1), Array(33, 1), Array(34, 1), Array(35, 1), Array(36, 1), Array(37, 1), Array( _
        38, 1), Array(39, 1), Array(40, 1), Array(41, 1), Array(42, 1), Array(43, 1), Array(44, 1), _
        Array(45, 1), Array(46, 1), Array(47, 1), Array(48, 1), Array(49, 1), Array(50, 1), Array( _
        51, 1), Array(52, 1), Array(53, 1), Array(54, 1), Array(55, 1), Array(56, 1), Array(57, 1), _
        Array(58, 1), Array(59, 1), Array(60, 1), Array(61, 1), Array(62, 1), Array(63, 1), Array( _
        64, 1), Array(65, 1), Array(66, 1), Array(67, 1), Array(68, 1), Array(69, 1), Array(70, 1), _
        Array(71, 1), Array(72, 1), Array(73, 1), Array(74, 1), Array(75, 1)), _
        TrailingMinusNumbers:=True
    Range(Selection, Selection.End(xlToRight)).Select
    Selection.Copy
    Range("A2").Select
    Selection.PasteSpecial Paste:=xlPasteAll, Operation:=xlNone, SkipBlanks:= _
        False, Transpose:=True
    Selection.End(xlUp).Select
    Range(Selection, Selection.End(xlToRight)).Select
    Application.CutCopyMode = False
    Selection.ClearContents
    Range("A2").Select
    Range(Selection, Selection.End(xlDown)).Select
    Selection.TextToColumns Destination:=Range("A2"), DataType:=xlDelimited, _
        TextQualifier:=xlDoubleQuote, ConsecutiveDelimiter:=True, Tab:=False, _
        Semicolon:=False, Comma:=False, Space:=False, Other:=True, OtherChar _
        :=",", FieldInfo:=Array(Array(1, 1), Array(2, 1), Array(3, 1)), _
        TrailingMinusNumbers:=True

End Sub
Sub ConcatColumns()
    Dim concatenator As String
Rng:
    On Error GoTo endSub
    Set myRange = Application.InputBox("select two columns which you want to concat:", "", Type:=8)
    If vbCancel = True Then
    Exit Sub
    End If

    If myRange.Columns.Count = 2 Then
    myRange.Select
    Else
    MsgBox "Select only 2 column range"
    GoTo Rng
    End If
    ActiveCell.Offset(0, 2).Select
    Selection.EntireColumn.Insert
'    MsgBox "My range:" & myRange.Address
    Selection.Offset(0, 0).Select
    concatenator = Application.InputBox("Please enter the concatenation value", Default:=".")
'    MsgBox "The concatenator:" & concatenator

    For i = 0 To myRange.Rows.Count - 1
        If ActiveCell.Offset(0, -2) = "" And ActiveCell.Offset(0, -1) = "" Then
            ActiveCell.Formula = ""
        Else
            ActiveCell.Formula = ActiveCell.Offset(0, -2) & concatenator & ActiveCell.Offset(0, -1)
        End If
        ActiveCell.Offset(1, 0).Select
    Next
endSub:
    Exit Sub
End Sub
