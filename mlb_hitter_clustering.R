# Conduct a query over multiple seaasons to compile stats 
# on hitters and perform a clustering analysis on them

library(ape)

# Connect to the SQL server hosting the data
library(RMySQL)
con <- dbConnect(RMySQL::MySQL(), dbname = "stats", 
                 host = "localhost", 
                 username = "noneya", 
                 password = "business", 
                 unix.socket = "/Applications/MAMP/tmp/mysql/mysql.sock", 
                 port = 3306)

# Set a vector of years to loop over, and perform the query for each year.
# Within each year, also remove duplicate rows and merge player stats if 
# they changed teams. Then merge the data for a single player in consecutive
# years.

yrs <- c(1996:2015)
for(j in 1:length(yrs)){
    # Here's my SQL query for each year.
    str1 <- "SELECT bat.playerID, mas.nameFirst, mas.nameLast, bat.yearID, 
    bat.teamID, bat.G, bat.AB, bat.R, bat.H, bat.2B, bat.3B, bat.HR, bat.RBI, 
    bat.SB, bat.CS, bat.BB, bat.SO, bat.IBB, bat.HBP, bat.SH, bat.SF, bat.GIDP, 
    app.G_all, app.G_p, fie.G as G_field, fie.PO as PO_field, fie.A as A_field, 
    fie.E as E_field 
    FROM batting as bat, master as mas, appearances as app, fielding as fie
    WHERE mas.playerID=bat.playerID AND app.playerID=mas.playerID AND 
    fie.playerID=mas.playerID AND bat.yearID="
    str2 <- as.character(yrs[j])
    str3 <- " AND fie.yearID="
    str4 <- ";"
    all.query <- dbGetQuery(con, paste(c(str1, str2, str3, str2, str4), collapse = ''))
    
    # First! Before going forward, change the 2B, 3B labels to "doubles" and "triples"
    colnames(all.query)[10] <- "doubles"
    colnames(all.query)[11] <- "triples"
    
    ### Some of these (IBB, HBP, SH, SF, GIDP, etc.) all are stored as 
    ### characters, and must be converted into floats/doubles. 
    all.query$AB <- as.double(all.query$AB)
    all.query$IBB <- as.double(all.query$IBB)
    all.query$HBP <- as.double(all.query$HBP)
    all.query$SH <- as.double(all.query$SH)
    all.query$SF <- as.double(all.query$SF)
    all.query$GIDP <- as.double(all.query$GIDP)
    
    # Something that needs to be dealt with. I have at least a few cases where a row is 
    # repeated, i.e., where we have the same player and team with their stats repeated.
    # Note that this is separate from the obvious cases where a player was traded, and so 
    # has their season stats split amongst multiple teams. I should really deal with both 
    # cases right here, though. 
    # Step 1: if there are duplicates of a playerID AND team, erase the duplicate row(s)
    # Step 2: if a player was on two teams, add their relevant stats together and make their 
    #         teamID a new one: TEAMID1/TEAMID2
    
    play.id.array <- all.query$playerID
    team.id.array <- all.query$teamID
    play.team.merge <- paste(play.id.array, team.id.array)
    all.query <- all.query[!duplicated(play.team.merge),]
    
    # Now, the remaining cases of duplicated playerIDs must be cases of a trade mid-season
    # or a player being released and picked up by another team
    the.dupes <- all.query[duplicated(all.query$playerID), ]
    for(i in 1:nrow(the.dupes)){
        # merge the stats with i index and the previous index, and update the teamID
        all.query[all.query$playerID == the.dupes$playerID[i], ][1,]$teamID <- 
            paste(c(all.query[all.query$playerID == the.dupes$playerID[i], ][1,]$teamID, 
                    "/", all.query[all.query$playerID == the.dupes$playerID[i], ][2,]$teamID), 
                  collapse = '')
        
        all.query[all.query$playerID == the.dupes$playerID[i], ][1,]$G <- 
            all.query[all.query$playerID == the.dupes$playerID[i], ][1,]$G + 
            all.query[all.query$playerID == the.dupes$playerID[i], ][2,]$G
        
        all.query[all.query$playerID == the.dupes$playerID[i], ][1,]$AB <- 
            all.query[all.query$playerID == the.dupes$playerID[i], ][1,]$AB + 
            all.query[all.query$playerID == the.dupes$playerID[i], ][2,]$AB
        
        all.query[all.query$playerID == the.dupes$playerID[i], ][1,]$R <- 
            all.query[all.query$playerID == the.dupes$playerID[i], ][1,]$R + 
            all.query[all.query$playerID == the.dupes$playerID[i], ][2,]$R
        
        all.query[all.query$playerID == the.dupes$playerID[i], ][1,]$H <- 
            all.query[all.query$playerID == the.dupes$playerID[i], ][1,]$H + 
            all.query[all.query$playerID == the.dupes$playerID[i], ][2,]$H
        
        all.query[all.query$playerID == the.dupes$playerID[i], ][1,]$doubles <- 
            all.query[all.query$playerID == the.dupes$playerID[i], ][1,]$doubles + 
            all.query[all.query$playerID == the.dupes$playerID[i], ][2,]$doubles
        
        all.query[all.query$playerID == the.dupes$playerID[i], ][1,]$triples <- 
            all.query[all.query$playerID == the.dupes$playerID[i], ][1,]$triples + 
            all.query[all.query$playerID == the.dupes$playerID[i], ][2,]$triples    
        
        all.query[all.query$playerID == the.dupes$playerID[i], ][1,]$HR <- 
            all.query[all.query$playerID == the.dupes$playerID[i], ][1,]$HR + 
            all.query[all.query$playerID == the.dupes$playerID[i], ][2,]$HR    
        
        all.query[all.query$playerID == the.dupes$playerID[i], ][1,]$RBI <- 
            all.query[all.query$playerID == the.dupes$playerID[i], ][1,]$RBI + 
            all.query[all.query$playerID == the.dupes$playerID[i], ][2,]$RBI
        
        all.query[all.query$playerID == the.dupes$playerID[i], ][1,]$SB <- 
            all.query[all.query$playerID == the.dupes$playerID[i], ][1,]$SB + 
            all.query[all.query$playerID == the.dupes$playerID[i], ][2,]$SB
        
        all.query[all.query$playerID == the.dupes$playerID[i], ][1,]$CS <- 
            all.query[all.query$playerID == the.dupes$playerID[i], ][1,]$CS + 
            all.query[all.query$playerID == the.dupes$playerID[i], ][2,]$CS    
        
        all.query[all.query$playerID == the.dupes$playerID[i], ][1,]$BB <- 
            all.query[all.query$playerID == the.dupes$playerID[i], ][1,]$BB + 
            all.query[all.query$playerID == the.dupes$playerID[i], ][2,]$BB    
        
        all.query[all.query$playerID == the.dupes$playerID[i], ][1,]$SO <- 
            all.query[all.query$playerID == the.dupes$playerID[i], ][1,]$SO + 
            all.query[all.query$playerID == the.dupes$playerID[i], ][2,]$SO    
        
        all.query[all.query$playerID == the.dupes$playerID[i], ][1,]$IBB <- 
            all.query[all.query$playerID == the.dupes$playerID[i], ][1,]$IBB + 
            all.query[all.query$playerID == the.dupes$playerID[i], ][2,]$IBB    
        
        all.query[all.query$playerID == the.dupes$playerID[i], ][1,]$HBP <- 
            all.query[all.query$playerID == the.dupes$playerID[i], ][1,]$HBP + 
            all.query[all.query$playerID == the.dupes$playerID[i], ][2,]$HBP    
        
        all.query[all.query$playerID == the.dupes$playerID[i], ][1,]$SH <- 
            all.query[all.query$playerID == the.dupes$playerID[i], ][1,]$SH + 
            all.query[all.query$playerID == the.dupes$playerID[i], ][2,]$SH
        
        all.query[all.query$playerID == the.dupes$playerID[i], ][1,]$SF <- 
            all.query[all.query$playerID == the.dupes$playerID[i], ][1,]$SF + 
            all.query[all.query$playerID == the.dupes$playerID[i], ][2,]$SF    
        
        all.query[all.query$playerID == the.dupes$playerID[i], ][1,]$GIDP <- 
            all.query[all.query$playerID == the.dupes$playerID[i], ][1,]$GIDP + 
            all.query[all.query$playerID == the.dupes$playerID[i], ][2,]$GIDP    

        all.query[all.query$playerID == the.dupes$playerID[i], ][1,]$G_all <- 
            all.query[all.query$playerID == the.dupes$playerID[i], ][1,]$G_all + 
            all.query[all.query$playerID == the.dupes$playerID[i], ][2,]$G_all

        all.query[all.query$playerID == the.dupes$playerID[i], ][1,]$G_p <- 
            all.query[all.query$playerID == the.dupes$playerID[i], ][1,]$G_p + 
            all.query[all.query$playerID == the.dupes$playerID[i], ][2,]$G_p

        all.query[all.query$playerID == the.dupes$playerID[i], ][1,]$G_field <- 
            all.query[all.query$playerID == the.dupes$playerID[i], ][1,]$G_field + 
            all.query[all.query$playerID == the.dupes$playerID[i], ][2,]$G_field

        all.query[all.query$playerID == the.dupes$playerID[i], ][1,]$PO_field <- 
            all.query[all.query$playerID == the.dupes$playerID[i], ][1,]$PO_field + 
            all.query[all.query$playerID == the.dupes$playerID[i], ][2,]$PO_field

        all.query[all.query$playerID == the.dupes$playerID[i], ][1,]$A_field <- 
            all.query[all.query$playerID == the.dupes$playerID[i], ][1,]$A_field + 
            all.query[all.query$playerID == the.dupes$playerID[i], ][2,]$A_field

        all.query[all.query$playerID == the.dupes$playerID[i], ][1,]$E_field <- 
            all.query[all.query$playerID == the.dupes$playerID[i], ][1,]$E_field + 
            all.query[all.query$playerID == the.dupes$playerID[i], ][2,]$E_field
    }
    
    ### Finally, remove all duplicates now that the stats have been merged!
    all.query <- all.query[!duplicated(all.query$playerID),]

    # If this is the first time, just store the overall.query as the current 
    # query results. If not, then we've gotta do some merging of the tables
    if(j == 1){
        overall.query <- all.query
    }
    if(j > 1){
        overall.new <- rbind(overall.query, all.query)
        the.dupes <- overall.new[duplicated(overall.new$playerID), ]
        for(i in 1:nrow(the.dupes)){
            # merge the stats with i index and the previous index, and update the teamID
            overall.new[overall.new$playerID == the.dupes$playerID[i], ][1,]$G <- 
                overall.new[overall.new$playerID == the.dupes$playerID[i], ][1,]$G + 
                overall.new[overall.new$playerID == the.dupes$playerID[i], ][2,]$G
            
            overall.new[overall.new$playerID == the.dupes$playerID[i], ][1,]$AB <- 
                overall.new[overall.new$playerID == the.dupes$playerID[i], ][1,]$AB + 
                overall.new[overall.new$playerID == the.dupes$playerID[i], ][2,]$AB
            
            overall.new[overall.new$playerID == the.dupes$playerID[i], ][1,]$R <- 
                overall.new[overall.new$playerID == the.dupes$playerID[i], ][1,]$R + 
                overall.new[overall.new$playerID == the.dupes$playerID[i], ][2,]$R
            
            overall.new[overall.new$playerID == the.dupes$playerID[i], ][1,]$H <- 
                overall.new[overall.new$playerID == the.dupes$playerID[i], ][1,]$H + 
                overall.new[overall.new$playerID == the.dupes$playerID[i], ][2,]$H
            
            overall.new[overall.new$playerID == the.dupes$playerID[i], ][1,]$doubles <- 
                overall.new[overall.new$playerID == the.dupes$playerID[i], ][1,]$doubles + 
                overall.new[overall.new$playerID == the.dupes$playerID[i], ][2,]$doubles
            
            overall.new[overall.new$playerID == the.dupes$playerID[i], ][1,]$triples <- 
                overall.new[overall.new$playerID == the.dupes$playerID[i], ][1,]$triples + 
                overall.new[overall.new$playerID == the.dupes$playerID[i], ][2,]$triples    
            
            overall.new[overall.new$playerID == the.dupes$playerID[i], ][1,]$HR <- 
                overall.new[overall.new$playerID == the.dupes$playerID[i], ][1,]$HR + 
                overall.new[overall.new$playerID == the.dupes$playerID[i], ][2,]$HR    
            
            overall.new[overall.new$playerID == the.dupes$playerID[i], ][1,]$RBI <- 
                overall.new[overall.new$playerID == the.dupes$playerID[i], ][1,]$RBI + 
                overall.new[overall.new$playerID == the.dupes$playerID[i], ][2,]$RBI
            
            overall.new[overall.new$playerID == the.dupes$playerID[i], ][1,]$SB <- 
                overall.new[overall.new$playerID == the.dupes$playerID[i], ][1,]$SB + 
                overall.new[overall.new$playerID == the.dupes$playerID[i], ][2,]$SB
            
            overall.new[overall.new$playerID == the.dupes$playerID[i], ][1,]$CS <- 
                overall.new[overall.new$playerID == the.dupes$playerID[i], ][1,]$CS + 
                overall.new[overall.new$playerID == the.dupes$playerID[i], ][2,]$CS    
            
            overall.new[overall.new$playerID == the.dupes$playerID[i], ][1,]$BB <- 
                overall.new[overall.new$playerID == the.dupes$playerID[i], ][1,]$BB + 
                overall.new[overall.new$playerID == the.dupes$playerID[i], ][2,]$BB    
            
            overall.new[overall.new$playerID == the.dupes$playerID[i], ][1,]$SO <- 
                overall.new[overall.new$playerID == the.dupes$playerID[i], ][1,]$SO + 
                overall.new[overall.new$playerID == the.dupes$playerID[i], ][2,]$SO    
            
            overall.new[overall.new$playerID == the.dupes$playerID[i], ][1,]$IBB <- 
                overall.new[overall.new$playerID == the.dupes$playerID[i], ][1,]$IBB + 
                overall.new[overall.new$playerID == the.dupes$playerID[i], ][2,]$IBB    
            
            overall.new[overall.new$playerID == the.dupes$playerID[i], ][1,]$HBP <- 
                overall.new[overall.new$playerID == the.dupes$playerID[i], ][1,]$HBP + 
                overall.new[overall.new$playerID == the.dupes$playerID[i], ][2,]$HBP    
            
            overall.new[overall.new$playerID == the.dupes$playerID[i], ][1,]$SH <- 
                overall.new[overall.new$playerID == the.dupes$playerID[i], ][1,]$SH + 
                overall.new[overall.new$playerID == the.dupes$playerID[i], ][2,]$SH
            
            overall.new[overall.new$playerID == the.dupes$playerID[i], ][1,]$SF <- 
                overall.new[overall.new$playerID == the.dupes$playerID[i], ][1,]$SF + 
                overall.new[overall.new$playerID == the.dupes$playerID[i], ][2,]$SF    
            
            overall.new[overall.new$playerID == the.dupes$playerID[i], ][1,]$GIDP <- 
                overall.new[overall.new$playerID == the.dupes$playerID[i], ][1,]$GIDP + 
                overall.new[overall.new$playerID == the.dupes$playerID[i], ][2,]$GIDP

            overall.new[overall.new$playerID == the.dupes$playerID[i], ][1,]$G_all <- 
                overall.new[overall.new$playerID == the.dupes$playerID[i], ][1,]$G_all + 
                overall.new[overall.new$playerID == the.dupes$playerID[i], ][2,]$G_all
            
            overall.new[overall.new$playerID == the.dupes$playerID[i], ][1,]$G_p <- 
                overall.new[overall.new$playerID == the.dupes$playerID[i], ][1,]$G_p + 
                overall.new[overall.new$playerID == the.dupes$playerID[i], ][2,]$G_p
            
            overall.new[overall.new$playerID == the.dupes$playerID[i], ][1,]$G_field <- 
                overall.new[overall.new$playerID == the.dupes$playerID[i], ][1,]$G_field + 
                overall.new[overall.new$playerID == the.dupes$playerID[i], ][2,]$G_field
            
            overall.new[overall.new$playerID == the.dupes$playerID[i], ][1,]$PO_field <- 
                overall.new[overall.new$playerID == the.dupes$playerID[i], ][1,]$PO_field + 
                overall.new[overall.new$playerID == the.dupes$playerID[i], ][2,]$PO_field
            
            overall.new[overall.new$playerID == the.dupes$playerID[i], ][1,]$A_field <- 
                overall.new[overall.new$playerID == the.dupes$playerID[i], ][1,]$A_field + 
                overall.new[overall.new$playerID == the.dupes$playerID[i], ][2,]$A_field
            
            overall.new[overall.new$playerID == the.dupes$playerID[i], ][1,]$E_field <- 
                overall.new[overall.new$playerID == the.dupes$playerID[i], ][1,]$E_field + 
                overall.new[overall.new$playerID == the.dupes$playerID[i], ][2,]$E_field
        }
        
        # Now take out the duplicates, and save it as overall.query and move on
        overall.query <- overall.new[!duplicated(overall.new$playerID),]
    }
}

# Okay, one little thing here that's kind of annoying, there are two players named 
# "Alex Gonzalez", having playerIDs gonzaal01 and gonzaal02. The player with ID 
# gonzaal01 is Alex S. Gonzalez, so I would just add a " S." to the first name string.
overall.query$nameFirst[overall.query$playerID == "gonzaal01"] <- paste("Alex", " S.")

# Select just those hitters who have more than 3000 AB over this time period
hitters <- overall.query[overall.query$AB > 3000,]

nseasons <- hitters$G/162.0
sb_per_season <- (hitters$SB/nseasons)/max(hitters$SB/nseasons)
sing <- hitters$H - hitters$doubles - hitters$triples - hitters$HR
obp <- (hitters$H + hitters$BB + hitters$HBP)/(hitters$AB + hitters$BB + hitters$HBP + hitters$SF)
tb <- sing + (2.0*hitters$doubles) + (3.0*hitters$triples) + (4.0*hitters$HR)
ba <- hitters$H/hitters$AB
slg <- tb/hitters$AB
ops <- obp+slg
iso <- slg-ba
so_per_ab <- (hitters$SO/hitters$AB)/max(hitters$SO/hitters$AB)
hr_per_ab <- (hitters$HR/hitters$AB)/max(hitters$HR/hitters$AB)
bb_per_ab <- (hitters$BB/hitters$AB)/max(hitters$BB/hitters$AB)
doubles_per_ab <- (hitters$doubles/hitters$AB)/max(hitters$doubles/hitters$AB)
f_percentage <- (hitters$PO_field + hitters$A_field)/(hitters$PO_field + hitters$A_field + hitters$E_field)

# Now append all this stuff I just calculated to the data frame of hitters:
hitters <- cbind(hitters, sing, obp, 
                 tb, ba, slg, ops, 
                 iso, so_per_ab, hr_per_ab, 
                 doubles_per_ab, f_percentage, 
                 bb_per_ab, sb_per_season)
colnames(hitters)

# Before making plots, make a nice set of player names for easy display in the 
# upcoming figures
hitter.names <- rep("", times = nrow(hitters))

# Set the names equal to a concatenation of firstname and lastname
for(i in 1:nrow(hitters)){
    hitter.names[i] <- paste(c(hitters$nameFirst[i], " ", 
                               hitters$nameLast[i]), collapse='')
}

# Now onto the calculations. The stats I'll use come from 
# columns 34, 41, 36, 40, 38, and 39
hitter.data <- hitters[, c(34, 41, 36, 40, 38, 39)]

# Get the distance matrix between all stats for all players
d <- dist(hitter.data)

# Compute the hierarchical clustering for those distances
c <- hclust(d)

# Now select seven groups
g7 <- cutree(c, k = 7)

# Convert to a phylogenetic tree and assign labels as hitter names
phylo.tree <- as.phylo(c)
labels(phylo.tree) <- hitter.names

# Choose the color palette, with seven colors selected - one for each group
labelColors <- c("cadetblue4", "chocolate4", "darkblue", 
                 "magenta", "firebrick3", "black", "darkgoldenrod3")

# Make the fan plot, with colors corresponding to each group
plot(phylo.tree, 
     type = "fan", 
     cex=0.7,  
     tip.color = labelColors[g7])

# Now make a scatterplot matrix that displays all the data points for the 
# parameters I'm tracking in my clustering analysis
pairs(hitters[c(34, 41, 36, 40, 38, 39)], 
    main = "Scatterplot Matrix for Hitting Stats", 
    col = labelColors[unclass(g7)], 
    pch = 16, 
    cex = 1.0, 
    labels = c("OPS", "SB/season", 
               "SO/AB", "BB/AB", "2B/AB", 
               "Fielding Percentage"))

