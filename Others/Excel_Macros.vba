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
