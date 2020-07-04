Attribute VB_Name = "Module1"
Sub AddWorksheetsFromSelection()
Attribute AddWorksheetsFromSelection.VB_Description = "The macro essentially grabs each cell in your selection, creates a new worksheet, and then renames that worksheet according to whatever was in the cell."
Attribute AddWorksheetsFromSelection.VB_ProcData.VB_Invoke_Func = "q\n14"
'
' AddWorksheetsFromSelection Macro
' The macro essentially grabs each cell in your selection, creates a new worksheet,
' and then renames that worksheet according to whatever was in the cell.
'
' Keyboard Shortcut: Ctrl+q
'
    Dim CurSheet As Worksheet
    Dim Source As Range
    Dim c As Range

    Set CurSheet = ActiveSheet
    Set Source = Selection.Cells
    Application.ScreenUpdating = False

    For Each c In Source
        sName = Trim(c.Text)
        If Len(sName) > 0 Then
                Worksheets.Add After:=Worksheets(Worksheets.Count)
                ActiveSheet.Name = sName
                Range("$A$1").Value = "Date"
                Range("$A$1").Font.Bold = True
                Range("$B$1").Value = "Timestamp"
                Range("$B$1").Font.Bold = True
        End If
    Next c

    CurSheet.Activate
    Application.ScreenUpdating = True

End Sub
Sub PasteFromClipboardAndTransposeAndRemoveSchema()
'
' PasteFromClipboardAndTransposeAndRemoveSchema Macro
' Plain text copied from external application can be Pasted in to current sheet and TransposeAndRemoveSchema
'
' Keyboard Shortcut: Ctrl+u
'
    Dim ft, ret As String
    
    Range("A2").Select
    ActiveSheet.Paste
    Selection.Copy
    Range("A3").Select
    Selection.PasteSpecial Paste:=xlPasteAll, Operation:=xlNone, SkipBlanks:= _
        False, Transpose:=True
    ret = Range("A3").Value
    ft = Split(ret, ".")(0)
    Selection.Replace What:=ft & ".", Replacement:="", _
        LookAt:=xlPart, SearchOrder:=xlByRows, MatchCase:=False, SearchFormat:= _
        False, ReplaceFormat:=False
    Range("A2").Select
'    Range(Selection, Selection.End(xlToRight)).Select
'    Application.CutCopyMode = False
'    Selection.ClearContents
    Rows("2:2").Select
    Selection.Delete Shift:=xlUp
    Range("A2").Select
    ActiveWorkbook.Save
End Sub


