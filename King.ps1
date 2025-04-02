Write-Host "Searching for files..." -ForegroundColor Green

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
    Dependencies
}

$global:CurrentState = [TableSearchStates]::Searching
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
        [Parameter(Mandatory = $true)][string]$Line
    )
    if ($Line -match 'SET IDENTITY_INSERT [dbo].[User] OFF') {
        $global:CurrentState = [OutputFileState]::WaitingSeparator
    }
    if ($Line -match 'INSERT INTO') {
        $global:CurrentState = [OutputFileState]::Dependencies
    }
}

function GetTablesOrderedList {
    param (
        [Parameter(Mandatory = $true)][System.Collections.ArrayList]$FileList
    )
    if ($FileList.Count -eq 0) {
        Write-Host "No files found."
    }
    else {
        Write-Host "Found $($FileList.Count) files."
        foreach ($file in $FileList) {
            $global:CurrentState = [TableSearchStates]::Searching
            $Content = Get-Content -Path $file
            $CurrentTableName = GetTableNameFromFileName -FileName $(Get-Item $file).Name
            $FoundReferece = $false
            Write-Host "Processing file for: $CurrentTableName"
            foreach ($Line in $Content) {
                if ([string]::IsNullOrWhiteSpace($Line)) {
                    continue
                }
                ProcessTableReferenceState -Line $Line
                if ((-not ($FoundReferece)) -and ($global:CurrentState -eq [TableSearchStates]::TableFound)) {
                    $FoundReferece = $true
                }
                ProcessOutputFileState -Line $Line
            }
            if (-not $FoundReferece) {
                AddTableReferenceControl -CurrentTableName $CurrentTableName
            }
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

$Time = [Diagnostics.Stopwatch]::StartNew()
$SqlFiles = GetTableSqlFiles
GetTablesOrderedList -FileList @($SqlFiles)
foreach ($Table in $global:AllTables) {
    $CurrentTableLevel = GetTableReferenceLevel -Level 0 -TableName $Table
    # Write-Host "Table: $Table, Level: $CurrentTableLevel"
    AddReferenceOnLevel -TableName $Table -Level $CurrentTableLevel
}
PrettyPrintTablesList
$Time.Stop()
Write-Host "Execution Time: $([math]::Round($Time.Elapsed.TotalSeconds, 2)) seconds" -ForegroundColor Green
