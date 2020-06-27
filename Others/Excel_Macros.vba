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
