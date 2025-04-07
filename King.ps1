# Ensure HashSet is available
Add-Type -TypeDefinition @"
using System.Collections.Generic;
"@

enum TableSearchStates {
    Searching
    FKFound
    ReferenceFound
    TableFound
}

enum OutputFileState {
    DropCreateInsert
    WaitingSeparator
    SeparatorFound
    Dependencies
}

$global:OutputWriter = $null
$global:ShouldWriteFile = $true

$global:DCIPath = "DCI"
$global:RefsPath = "Refs"
$global:IsRefsInitiated = $false

$global:ExecutionOrder = @("Schema", $global:DCIPath, $global:RefsPath, "View", "UserDefinedFunction", "StoredProcedure", "Users")

$global:CurrentState = [TableSearchStates]::Searching
$global:OutputState = [OutputFileState]::DropCreateInsert
$global:AllTables = [System.Collections.ArrayList]::new()
$global:ReferencedTablesList = [System.Collections.ArrayList]::new()
$global:TablesMap = [System.Collections.Hashtable]::new()
$global:TableReferencedByMap = [System.Collections.Hashtable]::new()

function GetTableSqlFiles {
    return Get-ChildItem -Path "." -Recurse -File -Filter "*Table.sql" -ErrorAction SilentlyContinue
}

function SearchForeignKey {
    param (
        [Parameter(Mandatory = $true)][string]$CurrentLine
    )
    if ($CurrentLine -match 'FOREIGN KEY') {
        # Write-Host "[FK] Line content: $CurrentLine"
        $global:CurrentState = [TableSearchStates]::FKFound
    }
}

function SearchReference {
    param (
        [Parameter(Mandatory = $true)][string]$CurrentLine
    )
    if ($CurrentLine -match 'REFERENCES') {
        # Write-Host "[REF] Line content: $CurrentLine"
        $global:CurrentState = [TableSearchStates]::ReferenceFound
    }
}

function ExtractReferencedTable {
    param (
        [Parameter(Mandatory = $true)][string]$CurrentLine
    )
    if ($CurrentLine -match 'REFERENCES\s+\[?\w+\]?\.\[?(\w+)\]') {
        $ReferencedTable = $matches[1]
        $global:CurrentState = [TableSearchStates]::TableFound
        return $ReferencedTable
    }
    return $null
}

function AddTableReferenceControl {
    param (
        [Parameter(Mandatory = $true)][string]$CurrentTableName,
        [Parameter(Mandatory = $false)][string]$ReferencedTableName
    )
    # Add table to the list of all tables
    if (-not ($global:AllTables -contains $CurrentTableName)) {
        $global:AllTables.Add($CurrentTableName) | Out-Null
    }
    if ([string]::IsNullOrWhiteSpace($ReferencedTableName)) {
        return
    }
    # Add control of the table reference
    if ($global:TablesMap.ContainsKey($CurrentTableName)) {
        $global:TablesMap[$CurrentTableName].Add($ReferencedTableName) | Out-Null
    }
    else {
        $global:TablesMap.Add($CurrentTableName, [System.Collections.Generic.HashSet[string]]::new())
        $global:TablesMap[$CurrentTableName].Add($ReferencedTableName) | Out-Null
    }
    # Add control of the referenced by tables
    if ($global:TableReferencedByMap.ContainsKey($ReferencedTableName)) {
        $global:TableReferencedByMap[$ReferencedTableName].Add($CurrentTableName) | Out-Null
    }
    else {
        $global:TableReferencedByMap.Add($ReferencedTableName, [System.Collections.Generic.HashSet[string]]::new())
        $global:TableReferencedByMap[$ReferencedTableName].Add($CurrentTableName) | Out-Null
    }
}

function AddReferenceOnLevel {
    param (
        [Parameter(Mandatory = $true)][string]$TableName,
        [Parameter(Mandatory = $true)][int]$Level
    )
    if ($global:ReferencedTablesList.Count -le $Level) {
        foreach ($i in $global:ReferencedTablesList.Count..$Level) {
            $global:ReferencedTablesList.Add([System.Collections.ArrayList]::new()) | Out-Null
        }
    }
    $global:ReferencedTablesList[$Level].add($TableName) | Out-Null
}

function GetTableReferenceLevel {
    param (
        [Parameter(Mandatory = $true)][Int16]$Level,
        [Parameter(Mandatory = $true)][string]$TableName
    )
    if ($global:TableReferencedByMap.ContainsKey($TableName)) {
        $HighestLevel = $Level
        foreach ($Table in $global:TableReferencedByMap[$TableName]) {
            $NewLevel = GetTableReferenceLevel -Level ($Level + 1) -TableName $Table
            if ($NewLevel -gt $HighestLevel) {
                $HighestLevel = $NewLevel
            }
        }
        $Level = $HighestLevel
    }
    else {
        return $Level
    }
    return $Level
}

function GetTableNameFromFileName {
    param (
        [Parameter(Mandatory = $true)][string]$FileName
    )
    
    if ($FileName -match '^\w+\.(\w+)\.\w+\.sql$') {
        return $matches[1]
    }
    Write-Host "No match found for table name in file name." -ForegroundColor Red
    exit 101
}

function ProcessTableReferenceState {
    param (
        [Parameter(Mandatory = $true)][string]$Line
    )
    $ReferencedTable = $null
    if ($global:CurrentState -eq [TableSearchStates]::Searching) {
        SearchForeignKey -CurrentLine $Line
    }
    if ($global:CurrentState -eq [TableSearchStates]::FKFound) {
        SearchReference -CurrentLine $Line
    }
    if ($global:CurrentState -eq [TableSearchStates]::ReferenceFound) {
        $ReferencedTable = ExtractReferencedTable -CurrentLine $Line
    }
    if ($global:CurrentState -eq [TableSearchStates]::TableFound) {
        if ($null -eq $ReferencedTable) {
            Write-Host "Referenced table is null. Exiting." -ForegroundColor Red
            exit 100
        }
        AddTableReferenceControl -CurrentTableName $CurrentTableName -ReferencedTableName $ReferencedTable
        $global:CurrentState = [TableSearchStates]::Searching
    }
}

function ProcessOutputFileState {
    param (
        [Parameter(Mandatory = $true)][string]$Line,
        [Parameter(Mandatory = $true)][string]$TableName
    )
    # if ($global:OutputState -eq [OutputFileState]::DropCreateInsert) {
    #     if ($Line -match 'SET IDENTITY_INSERT \[.*?\]\.\[.*?\] OFF') {
    #         $global:OutputState = [OutputFileState]::WaitingSeparator
    #     }
    # }
    if ($global:OutputState -eq [OutputFileState]::DropCreateInsert) {
        if ($Line -match 'CREATE TABLE') {
            $global:OutputState = [OutputFileState]::WaitingSeparator
        }
    }
    if ($global:OutputState -eq [OutputFileState]::WaitingSeparator) {
        if ($Line.StartsWith('ALTER TABLE')) {
            $global:OutputState = [OutputFileState]::Dependencies
        }
    }
    # if ($global:OutputState -eq [OutputFileState]::SeparatorFound) {
    #     if (-not ($Line -match 'GO')) {
    #         $global:OutputState = [OutputFileState]::Dependencies
    #     }
    # }
    if ($global:OutputState -ne [OutputFileState]::Dependencies) {
        if ($null -eq $global:OutputWriter) {
            $OutputFile = "$($global:DCIPath)$([System.IO.Path]::DirectorySeparatorChar)$($TableName).$($global:DCIPath).sql"
            if (Test-Path -Path $OutputFile) {
                $global:ShouldWriteFile = $false
            } else {
                $global:OutputWriter = [System.IO.StreamWriter]::new($OutputFile, $false, [System.Text.Encoding]::UTF8)
            }
        }
        if ($global:ShouldWriteFile) {
            $global:OutputWriter.WriteLine($Line)
        }
    } else {
        if (-not ($global:IsRefsInitiated)) {
            $global:IsRefsInitiated = $true
            $global:ShouldWriteFile = $true
            $Line = "USE [SIMA]`r`nGO`r`n$($Line)"
            CloseOutputWriter
            $OutputFile = "$($global:RefsPath)$([System.IO.Path]::DirectorySeparatorChar)$($TableName).$($global:RefsPath).sql"
            if (Test-Path -Path $OutputFile) {
                $global:ShouldWriteFile = $false
            } else {
                $global:OutputWriter = [System.IO.StreamWriter]::new($OutputFile, $false, [System.Text.Encoding]::UTF8)
            }
        }
        if ($global:ShouldWriteFile) {
            $global:OutputWriter.WriteLine($Line)
        }
    }
}

function ResetStates {
    $global:CurrentState = [TableSearchStates]::Searching
    $global:OutputState = [OutputFileState]::DropCreateInsert
    $global:IsRefsInitiated = $false
    $global:OutputWriter = $null
    $global:ShouldWriteFile = $true
}

function CloseOutputWriter {
    if ($global:OutputWriter -ne $null) {
        $global:OutputWriter.Close()
        $global:OutputWriter.Dispose()
        $global:OutputWriter = $null
    }
}

function FinishFileProcessing {
    CloseOutputWriter
}

function GenerateTablesOrderedList {
    param (
        [Parameter(Mandatory = $true)][System.Collections.ArrayList]$FileList
    )
    if ($FileList.Count -eq 0) {
        Write-Host "No files found."
    }
    else {
        # Write-Host "Found $($FileList.Count) files."
        foreach ($file in $FileList) {
            ResetStates
            $Reader = [System.IO.StreamReader]::new($file)
            $CurrentTableName = GetTableNameFromFileName -FileName $(Get-Item $file).Name
            $FoundReferece = $false
            Write-Host "Processing file for: $CurrentTableName"
            while ($null -ne ($Line = $Reader.ReadLine())) {
                if ([string]::IsNullOrWhiteSpace($Line)) {
                    continue
                }
                ProcessTableReferenceState -Line $Line
                if ((-not ($FoundReferece)) -and ($global:CurrentState -eq [TableSearchStates]::TableFound)) {
                    $FoundReferece = $true
                }
                ProcessOutputFileState -Line $Line -TableName $CurrentTableName
            }
            $Reader.Close()
            FinishFileProcessing -CurrentTableName $CurrentTableName
            if (-not $FoundReferece) {
                AddTableReferenceControl -CurrentTableName $CurrentTableName
            }
        }
    }
}

function RunSQLFile {
    param (
        [Parameter(Mandatory=$true)][string]$Type,
        [Parameter(Mandatory=$true)][string]$File
    )
    if (-not (Test-Path -Path $File)) {
        return
    }

    Write-Host "Arquivo: $File"

    #################################################
    #
    #       EXECUÇÃO LOCAL
    #
    #################################################

    $hostname = "127.0.0.1"
    $port = "1444"
    $login = "tulio"
    $pass = "12345678"
    sqlcmd -S "$hostname,$port" -U $login -P $pass -x -i "$File" >> "$($Type)$([System.IO.Path]::DirectorySeparatorChar)${Type}_queries.log"

    #################################################
    #
    #       EXECUÇÃO EM HOMOLOGAÇÃO
    #
    #################################################

    # $hostname = "10.100.10.65"
    # sqlcmd -S $hostname -E -x -i "$File" >> "${Type}_queries.log"
}

function ExecuteScripts {
    foreach ($Type in $global:ExecutionOrder) {
        if ($Type -eq $global:DCIPath) {
            for ($i = 0; $i -lt $global:ReferencedTablesList.Count; $i++) {
                $Tables = $global:ReferencedTablesList[$i]
                foreach ($Table in $Tables) {
                    $File = ".$([System.IO.Path]::DirectorySeparatorChar)$($global:DCIPath)$([System.IO.Path]::DirectorySeparatorChar)$($Table).$($global:DCIPath).sql"
                    RunSQLFile -Type $Type -File $File
                }
            }
            continue
        }
        if ($Type -eq $global:RefsPath) {
            for ($i = $global:ReferencedTablesList.Count - 1; $i -ge 0; $i--) {
                $Tables = $global:ReferencedTablesList[$i]
                foreach ($Table in $Tables) {
                    $File = ".$([System.IO.Path]::DirectorySeparatorChar)$($global:RefsPath)$([System.IO.Path]::DirectorySeparatorChar)$($Table).$($global:RefsPath).sql"
                    RunSQLFile -Type $Type -File $File
                }
            }
            continue
        }
        $Files = Get-ChildItem -Path $Type -File -Filter "*.$($Type).sql"
        foreach ($File in $Files) {
            $TableName = GetTableNameFromFileName -FileName $(Get-Item $File).Name
            RunSQLFile -Type $Type -TableName $TableName
        }
    }
}

function PrettyPrintTablesList {
    Write-Host ""
    Write-Host "Total Referenced Levels: $($global:ReferencedTablesList.Count)"
    $LevelCount = 0
    foreach ($Tables in $global:ReferencedTablesList) {
        Write-Host "Level ${LevelCount}:" -ForegroundColor Blue
        foreach ($Table in $Tables) {
            Write-Host "... $Table"
        }
        $LevelCount++
    }
}

function CreateFolderOrCleanIt {
    param (
        [Parameter(Mandatory = $true)][string]$Path
    )
    if (-not (Test-Path -Path $Path)) {
        New-Item -ItemType Directory -Path $Path | Out-Null
    } else{
        Get-ChildItem -Path $Path -Recurse | Remove-Item -Force -Recurse
    }
}

function MoveFilesToFolders {
    param (
        [Parameter(Mandatory = $true)][string]$Filter
    )
    $Files = Get-ChildItem -Path "." -File -Filter "*.$($Filter).sql"
    foreach ($File in $Files) {
        $DestinationPath = "$($Filter)$([System.IO.Path]::DirectorySeparatorChar)"
        Move-Item -Path $File -Destination $DestinationPath
    }
}

function GenerateTablesLevels {
    foreach ($Table in $global:AllTables) {
        $CurrentTableLevel = GetTableReferenceLevel -Level 0 -TableName $Table
        # Write-Host "Table: $Table, Level: $CurrentTableLevel"
        AddReferenceOnLevel -TableName $Table -Level $CurrentTableLevel
    }
}

function CreateAllFoldersAndMoveFiles {
    foreach ($Type in $global:ExecutionOrder) {
        CreateFolderOrCleanIt -Path $Type
        MoveFilesToFolders -Filter $Type
    }
}

Write-Host "Running..." -ForegroundColor Green
CreateAllFoldersAndMoveFiles
$Time = [Diagnostics.Stopwatch]::StartNew()
GenerateTablesOrderedList -FileList @(GetTableSqlFiles)
GenerateTablesLevels
PrettyPrintTablesList
$Time.Stop()
Write-Host "Execution Time: $([math]::Round($Time.Elapsed.TotalSeconds, 2)) seconds" -ForegroundColor Green
ExecuteScripts