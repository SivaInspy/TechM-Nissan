Option Explicit
Sub Workflow_Clean()
'
' Workflow_Clean Macro
'
'

     'If something goes wrong, go to the errorhandler
    On Error GoTo ERRORHANDLER
     'Checks the document for excessive spaces between words
    With Selection
        .HomeKey Unit:=wdStory
        With .Find
            .ClearFormatting
            .Replacement.ClearFormatting
             'Here is where it is actually looking for spaces between words
            .Text = " [ ]@([! ])"
             'This line tells it to replace the excessive spaces with one space
            .Replacement.Text = " \1"
            .MatchWildcards = True
            .Wrap = wdFindStop
            .Format = False
            .Forward = True
             'execute the replace
            .Execute Replace:=wdReplaceAll
        End With

        With .Find
             'This time its looking for excessive spaces after a paragraph mark
            .Text = " ^p"
             'What to replace it with
            .Replacement.Text = "^p"
            .MatchWildcards = False
            .Wrap = wdFindStop
            .Format = False
            .Forward = True
             'Execute the replace
            .Execute Replace:=wdReplaceAll
        End With

        With .Find
             'This time its looking for excessive spaces after a paragraph mark
            .Text = "^p "
             'What to replace it with
            .Replacement.Text = "^p"
            .MatchWildcards = False
            .Wrap = wdFindStop
            .Format = False
            .Forward = True
             'Execute the replace
            .Execute Replace:=wdReplaceAll
        End With

        With .Find
            .ClearFormatting
            .Replacement.ClearFormatting
             'Here is where it is actually quoting the dates
            .Text = "([0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2} [A-Z]{3})"
             'This line tells it to replace the excessive spaces with one space
            .Replacement.Text = """\1"""
            .MatchWildcards = True
            .Wrap = wdFindStop
            .Format = False
            .Forward = True
             'execute the replace
            .Execute Replace:=wdReplaceAll
        End With

        With .Find
             'This time its looking for inserting CRON with space
            .Text = "CRON"
             'What to replace it with
            .Replacement.Text = """ CRON"
            .MatchWildcards = False
            .Wrap = wdFindStop
            .Format = False
            .Forward = True
             'Execute the replace
            .Execute Replace:=wdReplaceAll
        End With

        With .Find
             'This time its looking for RUNNING with space
            .Text = "RUNNING "
             'What to replace it with
            .Replacement.Text = " RUNNING """
            .MatchWildcards = False
            .Wrap = wdFindStop
            .Format = False
            .Forward = True
             'Execute the replace
            .Execute Replace:=wdReplaceAll
        End With

        With .Find
             'This time its looking for KILLED with space
            .Text = "KILLED "
             'What to replace it with
            .Replacement.Text = " KILLED """
            .MatchWildcards = False
            .Wrap = wdFindStop
            .Format = False
            .Forward = True
             'Execute the replace
            .Execute Replace:=wdReplaceAll
        End With

        With .Find
             'This time its looking for SUCCEEDED with space
            .Text = "SUCCEEDED "
             'What to replace it with
            .Replacement.Text = " SUCCEEDED """
            .MatchWildcards = False
            .Wrap = wdFindStop
            .Format = False
            .Forward = True
             'Execute the replace
            .Execute Replace:=wdReplaceAll
        End With

        With .Find
             'This time its looking for SUCCEEDED with space
            .Text = " MINUTE """
             'What to replace it with
            .Replacement.Text = """ MINUTE """
            .MatchWildcards = False
            .Wrap = wdFindStop
            .Format = False
            .Forward = True
             'Execute the replace
            .Execute Replace:=wdReplaceAll
        End With

        With .Find
             'This time its looking for SUCCEEDED with space
            .Text = " MONTH """
             'What to replace it with
            .Replacement.Text = """ MONTH """
            .MatchWildcards = False
            .Wrap = wdFindStop
            .Format = False
            .Forward = True
             'Execute the replace
            .Execute Replace:=wdReplaceAll
        End With

        With .Find
             'This time its looking for SUCCEEDED with space
            .Text = " DAY """
             'What to replace it with
            .Replacement.Text = """ DAY """
            .MatchWildcards = False
            .Wrap = wdFindStop
            .Format = False
            .Forward = True
             'Execute the replace
            .Execute Replace:=wdReplaceAll
        End With

        With .Find
             'This time its looking for SUCCEEDED with space
            .Text = "NOT NULL"
             'What to replace it with
            .Replacement.Text = """NOT NULL"""
            .MatchWildcards = False
            .Wrap = wdFindStop
            .Format = False
            .Forward = True
             'Execute the replace
            .Execute Replace:=wdReplaceAll
        End With

        With .Find
            .ClearFormatting
            .Replacement.ClearFormatting
             'Here is where it is actually quoting the dates
            .Text = "([0-9]) - """
             'This line tells it to replace the excessive spaces with one space
            .Replacement.Text = "\1"" - """
            .MatchWildcards = True
            .Wrap = wdFindStop
            .Format = False
            .Forward = True
             'execute the replace
            .Execute Replace:=wdReplaceAll
        End With

    End With
ERRORHANDLER:
    With Selection
        .ExtendMode = False
        .HomeKey Unit:=wdStory
    End With
End Sub
