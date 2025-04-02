Write-Host "Searching for files..." -ForegroundColor Green

enum States {
    Searching
    FKFound
    ReferenceFound
    TableFound
}

$global:CurrentState = [States]::Searching
$global:AllTables = [System.Collections.ArrayList]::new()
$global:ReferencedTablesList = [System.Collections.ArrayList]::new()
$global:TablesMap = [System.Collections.Hashtable]::new()
$global:TableReferencedByMap = [System.Collections.Hashtable]::new()

function GetSqlFiles {
    return Get-ChildItem -Path "." -Recurse -File -Filter "*Table.sql" -ErrorAction SilentlyContinue
}

function SearchForeignKey {
    param (
        [Parameter(Mandatory = $true)][string]$CurrentLine
    )
    if ($CurrentLine -match 'FOREIGN KEY') {
        # Write-Host "[FK] Line content: $CurrentLine"
        $global:CurrentState = [States]::FKFound
    }
}

function SearchReference {
    param (
        [Parameter(Mandatory = $true)][string]$CurrentLine
    )
    if ($CurrentLine -match 'REFERENCES') {
        # Write-Host "[REF] Line content: $CurrentLine"
        $global:CurrentState = [States]::ReferenceFound
    }
}

function ExtractReferencedTable {
    param (
        [Parameter(Mandatory = $true)][string]$CurrentLine
    )
    if ($CurrentLine -match 'REFERENCES\s+\[?\w+\]?\.\[?(\w+)\]') {
        $ReferencedTable = $matches[1]
        $global:CurrentState = [States]::TableFound
        return $ReferencedTable
    }
    return $null
}

# function OrganizeTableOrder {
#     param (
#         [Parameter(Mandatory=$true)][string]$CurrentTableName,    
#         [Parameter(Mandatory=$true)][string]$ReferencedTableName,
#         [Parameter(Mandatory=$false)][System.Collections.ArrayList]$ReferencedTablesList
#     )
#     AddTableReferenceControl -CurrentTableName $CurrentTableName -ReferencedTableName $ReferencedTableName
#     Write-Host "[ORG] Current Table: $CurrentTableName, Referenced Table: $ReferencedTableName"
#     $TableReferenceLevel = GetTableReferenceLevel -Level 0 -TableName $ReferencedTableName
#     Write-Host "[ORG] Referenced Table Level is $TableReferenceLevel"
#     $ReferencedTablesList.Add($ReferencedTableName) | Out-Null
# }

function AddTableReferenceControl {
    param (
        [Parameter(Mandatory = $true)][string]$CurrentTableName,
        [Parameter(Mandatory = $false)][string]$ReferencedTableName
    )
    # Add table to the list of all tables
    if (-not ($global:AllTables -contains $CurrentTableName)) {
        $global:AllTables.Add($CurrentTableName) | Out-Null
    }
    if ([string]::IsNullOrWhiteSpace($Line)) {
        return
    }
    # Add control of the table reference
    if ($global:TablesMap.ContainsKey($CurrentTableName)) {
        if (-not ($global:TablesMap[$CurrentTableName] -contains $ReferencedTableName)) {
            $global:TablesMap[$CurrentTableName].Add($ReferencedTableName) | Out-Null
        }
    }
    else {
        $global:TablesMap.Add($CurrentTableName, [System.Collections.ArrayList]::new())
        $global:TablesMap[$CurrentTableName].Add($ReferencedTableName) | Out-Null
    }
    # Add control of the referenced by tables
    if ($global:TableReferencedByMap.ContainsKey($ReferencedTableName)) {
        if (-not ($global:TableReferencedByMap[$ReferencedTableName] -contains $CurrentTableName)) {
            $global:TableReferencedByMap[$ReferencedTableName].Add($CurrentTableName) | Out-Null
        }
    }
    else {
        $global:TableReferencedByMap.Add($ReferencedTableName, [System.Collections.ArrayList]::new())
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
            $Content = Get-Content -Path $file
            $CurrentTableName = GetTableNameFromFileName -FileName $(Get-Item $file).Name
            $FoundReferece = $false
            # Write-Host "Processing file for: $CurrentTableName"
            foreach ($Line in $Content) {
                $ReferencedTable = $null
                if ([string]::IsNullOrWhiteSpace($Line)) {
                    continue
                }
                if ($global:CurrentState -eq [States]::Searching) {
                    SearchForeignKey -CurrentLine $Line
                }
                if ($global:CurrentState -eq [States]::FKFound) {
                    SearchReference -CurrentLine $Line
                }
                if ($global:CurrentState -eq [States]::ReferenceFound) {
                    $ReferencedTable = ExtractReferencedTable -CurrentLine $Line
                    $FoundReferece = $true
                }
                if ($global:CurrentState -eq [States]::TableFound) {
                    if ($null -eq $ReferencedTable) {
                        Write-Host "Referenced table is null. Exiting." -ForegroundColor Red
                        exit 100
                    }
                    AddTableReferenceControl -CurrentTableName $CurrentTableName -ReferencedTableName $ReferencedTable
                    $global:CurrentState = [States]::Searching
                }
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
$SqlFiles = GetSqlFiles
GetTablesOrderedList -FileList @($SqlFiles)
foreach ($Table in $global:AllTables) {
    $CurrentTableLevel = GetTableReferenceLevel -Level 0 -TableName $Table
    # Write-Host "Table: $Table, Level: $CurrentTableLevel"
    AddReferenceOnLevel -TableName $Table -Level $CurrentTableLevel
}
PrettyPrintTablesList
$Time.Stop()
Write-Host "Execution Time: $($Time.Elapsed.TotalSeconds) seconds" -ForegroundColor Green