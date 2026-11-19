function(x=NULL, y=NULL, cat=NULL, use=NULL, deg=c(1,2,3,4,5), quantiles=c(0.95, 0.99, 0.995, 0.999, 0.9999), conv=1e-8, max_iter=1000, input_file=NULL, output_file="C:/Users/samsonm/Documents/JunkWorking/Quantile_Regression_Report.pdf") {
  if (!is.null(input_file) && file.exists(input_file)) {
    input <- read.csv(input_file)
#  Manually select between Use1 and Use2
    data <- data.frame(x=input$AMSalinity, y=input$AMIDepth, cat=input$Region, use=input$Use2)
  } else {
    # Combine data into a data frame
    data <- data.frame(x=x, y=y, cat=cat, use=use)
  }

  # Check for duplicates
  duplicates <- duplicated(data)
  if (any(duplicates)) {
    warning("Duplicate records found in the dataset. Removing duplicates.")
    data <- data[!duplicates, ]
  }
  # Remove negative depths or flaged for inclusion
    data <- data[which(data$y > -0.1),]
    data <- data[which(data$use == 1),]


  # Print the output file path for debugging
  cat("Output file will be saved to:", output_file, "\n")
  
  # Set common x and y range for all plots
  x_range <- range(data$x)
  y_range <- range(data$y)
  
  # Data frame to store coefficients
  coef_list <- list()
  
  fit_quantile_regression <- function(data, deg, q, conv, max_iter) {
    lst <- lm(y ~ poly(x, deg, raw=TRUE), data=data)
    test <- 1
    count <- 0
    coef_old <- coef(lst)
    
    while (test > conv && count < max_iter) {
      count <- count + 1
      w <- ifelse(lst$residuals > 0, q, 1 - q)
      nxt <- lm(y ~ poly(x, deg, raw=TRUE), data=data, weights=w)
      coef_new <- coef(nxt)
      test <- max(abs(coef_new - coef_old))
      cat("Iteration:", count, "Convergence criteria (max change in coefficients):", test, "\n")
      coef_old <- coef_new
      lst <- nxt
    }
    
    return(lst)
  }

  plot_quantile_regression <- function(data, lst, main_title) {
    plot(data$x, data$y, main=main_title, xlab="Salinity (ppt)", ylab="Depth (m)", pch=19, col="black", cex=0.5, xlim=x_range, ylim=y_range)
    new_x <- seq(min(data$x), max(data$x), length.out=100)
    new_y <- predict(lst, newdata=data.frame(x=new_x))
    lines(new_x, new_y, col="blue", lwd=2)
#   points(data$x, data$y, cex=.25, col="red")
    coef_text <- paste("Coefficients:\n", paste(names(coef(lst)), round(coef(lst), 5), sep=" = ", collapse="\n"))
    mtext(coef_text, side=3, line=-3, adj=1, cex=0.7, col="darkgreen")
  }

  pdf(output_file, width=8, height=6)

  for (d in deg) {  
    for (q in quantiles) {
      cat("Fitting for the whole dataset with quantile:", q, "and degree:", d, "\n")
      lst_whole <- fit_quantile_regression(data, d, q, conv, max_iter)
      
      # Save coefficients and R-squared value to list
      r_squared <- summary(lst_whole)$r.squared
      coef_whole <- as.data.frame(t(coef(lst_whole)))
      coef_whole <- cbind(Category="Whole", Degree=d, Quantile=q, R_Squared=r_squared, coef_whole)
      coef_list <- append(coef_list, list(coef_whole))
      
      # Plot for the whole dataset
      plot_quantile_regression(data, lst_whole, paste("Quantile Regression (q =", q, ", deg =", d, ")\nWhole Dataset"))
      
      # Fit and plot for each category
      categories <- unique(data$cat)
      for (c in categories) {
        cat("\nFitting for category:", c, "with quantile:", q, "and degree:", d, "\n")
        data_cat <- subset(data, cat == c)
        lst_cat <- fit_quantile_regression(data_cat, d, q, conv, max_iter)
        
        # Save coefficients and R-squared value to list
        r_squared_cat <- summary(lst_cat)$r.squared
        coef_cat <- as.data.frame(t(coef(lst_cat)))
        coef_cat <- cbind(Category=c, Degree=d, Quantile=q, R_Squared=r_squared_cat, coef_cat)
        coef_list <- append(coef_list, list(coef_cat))
        
        # Plot for each category
        plot_quantile_regression(data_cat, lst_cat, paste("Quantile Regression (q =", q, ", deg =", d, ")\n", c))
      }
    }
  }

  # Close the PDF device
  dev.off()
  cat("All plots saved in:", output_file, "\n")
  
  # Get all unique column names
  all_colnames <- unique(unlist(lapply(coef_list, names)))
  
  # Standardize each data frame to have the same columns
  coef_list <- lapply(coef_list, function(df) {
    missing_cols <- setdiff(all_colnames, names(df))
    df[missing_cols] <- NA
    return(df[all_colnames])
  })
  
  # Combine the list of data frames into a single data frame
  coef_df <- do.call(rbind, coef_list)

write.csv(coef_df, file="C:/Users/samsonm/Documents/JunkWorking/Quantile_Reg_Coef.csv")
  
  return(coef_df)

}
